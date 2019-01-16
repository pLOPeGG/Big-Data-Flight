from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, functions as F, Row
import numpy as np
import pandas as pd
import itertools
import argparse
from functools import reduce

from airports import AirportFinder, get_european_airports


sc = SparkContext()
sc.addPyFile('src/utils.py')
sc.addPyFile('src/airports.py')
spark = SparkSession(sc)

labels = ["Lat", "Long", "PosTime", "GAlt", "Spd", "TAlt"]
labels_aliases = ["Lat", "Long", "Time", "Alt", "Speed", "TargetAlt"]
other_labels = ["Icao", "Op", "Engines", "Mil", "Cou", "Mdl"]


def filter_data(input_json: str):
    base_df = spark.read.json(input_json)
    # print(f"Loaded {input_json}: {base_df.count()} records")
    base_df = base_df.filter(
        reduce(lambda a, b: a & b, [base_df[l].isNotNull() for l in labels]))
    # print(f"Removed null lst_labels: remains {base_df.count()} records")

    df = base_df.filter(
        reduce(lambda a, b: a & b, [base_df[l].isNotNull() for l in other_labels]))
    # print(f"Removed null other_labels: remains {df.count()} records")

    df = df.filter((df.Lat > 36) & (df.Lat < 70) &
                   (df.Long > -26) & (df.Long < 34))
    # print(f"Applied Positional and European filters: remains {df.count()} records")

    df = df.filter(~df.Mlat)
    # print(f"Applied GPS Filter: remains {df.count()} records")
    return df
    pass


def group_icao(df: DataFrame, grp_cols, lst_cols, lst_cols_alias):
    # df = df.orderBy(df.PosTime)  # useless, order is not kept with groupBy
    df = df.select(*np.concatenate((grp_cols, lst_cols)))
    df_group = df.groupBy(*grp_cols)\
                 .agg(*[F.collect_list(df[c]).alias(c_name) for c, c_name in zip(lst_cols, lst_cols_alias)])
    # df_group = df_group.select('*', F.size(cols[0]).alias("Size"))

    # print(f"Groupes by ID: remains {df_group.count()} records")

    # df_group = df_group.orderBy(df_group["Size"].desc())
    return df_group


def sort_by_time(rdd: RDD):
    def map_sort_time(record):
        # TODO: beautify this with global column names
        alt, talt, lat, long, speed, time = record.Alt, record.TargetAlt, record.Lat, record.Long, record.Speed, record.Time
        # Magic sort in python
        if len(time) > 0:
            time, alt, talt, lat, long, speed = map(
                list, zip(*sorted(zip(time, alt, talt, lat, long, speed))))
        return Row(Icao=record.Icao,
                   Op=record.Op,
                   Engines=record.Engines,
                   Mil=record.Mil,
                   Cou=record.Cou,
                   Mdl=record.Mdl,
                   Lat=lat,
                   Long=long,
                   Alt=alt,
                   TargetAlt=talt,
                   Speed=speed,
                   Time=time)
    return rdd.map(map_sort_time)


def filter_big_time_step(rdd: RDD, min_step: float, max_step: float) -> RDD:
    """ Some plane sometimes pass out for a long time (1k sec and more) without moving.
    We remove those planes from the records (SHALL WE ???)
    """
    def map_get_max_time_step(record):
        time = record.Time
        max_step = 0
        min_step = 0
        if len(time) > 1:
            arr = [t2-t1 for t1, t2 in zip(time[:-1], time[1:])]
            max_step = np.max(arr)
            min_step = np.min(arr)
        return record, min_step, max_step

    rdd = rdd.map(map_get_max_time_step)

    rdd = rdd.filter(lambda r: r[1] >= min_step * 1000 and r[2] <= max_step * 1000) \
             .map(lambda r: r[0])  # remove from the record max and min steps

    # print(f"Applied filter on time steps of size {(min_step, max_step)}, remains {rdd.count()} records")
    return rdd
    pass


def flatten_trips(rdd: RDD,
                  airport: AirportFinder,
                  air_info,
                  sticky: float = float("inf"),
                  time_threshold: float = float("inf")) -> RDD:
    def flatmap_split_trips(record,
                            airport: AirportFinder = airport,
                            air_info=air_info,
                            sticky=sticky,
                            time_threshold=time_threshold):
        """ Split the record for each plane in multiple records:
        One for each trip the plane took.
        """
        alt, talt, lat, long, speed, time = record.Alt, record.TargetAlt, record.Lat, record.Long, record.Speed, record.Time
        # Ideas: - Apply low frequency filter on the altitude to
        #          remove noise from data ?
        #        - Do the same on Speed values ?

        # Get local minimums of the altitude -> this might be take off and landing

        if len(time) <= 2:
            return []

        # local_alt_bool = np.array([a < local_alt for a, local_alt in zip(alt,
        #                                                                  [airport.local_max_alt(pos, air_info) for pos in zip(lat, long)])])

        # split_indices = [i for i, (a, b) in enumerate(zip(np.gradient(np.sign(np.gradient(alt))) > 0,
        #                                                   local_alt_bool))
        #                  if a and b]
        ta_max = 0.5 * max(talt)

        local_alt_bool = np.array([a < local_alt and ta < ta_max
                                   for a, ta, local_alt in zip(alt,
                                                               talt,
                                                               [airport.local_max_alt(pos, air_info) for pos in zip(lat, long)])])

        split_indices = [i for i, (a, b, c) in enumerate(zip(np.gradient(np.sign(np.gradient(alt))) > 0,
                                                             np.gradient(np.sign(np.gradient(talt))) >= 0,
                                                             local_alt_bool))
                         if a and b and c]

        # TODO: Here compute distance airport - position and validate the landing.
        # TODO: if too big time shifts appears -> filter out ?
        #                                      -> land and cut ?

        corresponding_airports = [airport.closest_airport((lat[i], long[i]))
                                  for i in split_indices]

        couples = [(i, j)
                   for i, j in zip(split_indices[:-1], split_indices[1:])]

        trip_l = []
        for k, (i, j) in enumerate(couples):
            air_from = air_info[corresponding_airports[k]]
            air_to = air_info[corresponding_airports[k+1]]

            # Get real trips only
            if air_from == air_to:
                continue

            # A trip must last a bit
            if time[j] - time[i] < time_threshold:
                continue

            # TODO: control here for monotonicity of lat, long and not too long trip
            #       -> thoughts for monotonicity:
            #               -> real monotonicity (bad)
            #               -> having a direction (between airports), make sure at 
            #                  least 80% steps go into that direction
            #       -> cut into multiple trips in the middle where monotonicity fails! 

            row = Row(Icao=record.Icao,
                      Op=record.Op,
                      Engines=record.Engines,
                      Mil=record.Mil,
                      Cou=record.Cou,
                      Mdl=record.Mdl,
                      From=air_from,
                      To=air_to,
                      Lat=lat[i:j+1],
                      Long=long[i:j+1],
                      Alt=alt[i:j+1],
                      TargetAlt=talt[i:j+1],
                      Speed=speed[i:j+1],
                      Time=time[i:j+1],
                      )
            trip_l.append(row)
        return trip_l

    rdd = rdd.flatMap(flatmap_split_trips)
    # print(f"Applied trips recognition, remains {rdd.count()} records")
    return rdd


def get_clean_data(file) -> RDD:
    air_dict, air_loc, air_finder = get_european_airports()  # All info about airports
    df = filter_data(file)  # European filter

    df_grouped = group_icao(df,
                            other_labels,
                            labels,
                            labels_aliases)  # Group by ICAO

    # Return a rdd where each record is a Row
    rdd = sort_by_time(df_grouped.rdd)

    # rdd = filter_big_time_step(rdd, -1, float("inf"))  # Filter out ill formed time steps
    # print(rdd.take(5))
    rdd = flatten_trips(rdd, air_finder, air_dict, time_threshold=15*60*1000)

    return rdd


def main():
    parser = argparse.ArgumentParser(description='Do stuff')
    parser.add_argument('--path', '-p', default="./data/2017-01-01/2017-01-01-*.json",
                        help='Path to json data files')
    parser.add_argument('--save', '-s', default="pyspark_output",
                        help='Path to saved output')
    args = parser.parse_args()

    file_json = args.path
    save_dir = args.save

    rdd = get_clean_data(file_json)

    rdd.saveAsTextFile(save_dir)
    # print(rdd.count())
    # print(rdd.take(20))

    pass


if __name__ == '__main__':

    main()
