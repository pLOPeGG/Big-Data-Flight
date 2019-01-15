from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, functions as F, Row
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.basemap import Basemap
import itertools

from airports import AirportFinder, get_european_airports


sc = SparkContext()
sc.addPyFile('src/utils.py')
sc.addPyFile('src/airports.py')
spark = SparkSession(sc)

labels = ["Lat", "Long", "PosTime", "GAlt", "Spd"]
labels_names = ["Lat", "Long", "Time", "Alt", "Speed"]


def filter_data(input_json: str):
    base_df = spark.read.json(input_json)
    # print(f"Loaded {input_json}: {base_df.count()} records")
    df = base_df.filter(base_df.Lat.isNotNull() & base_df.Long.isNotNull() &
                        base_df.PosTime.isNotNull() & base_df.GAlt.isNotNull() &
                        base_df.Spd.isNotNull())
    df = df.filter((df.Lat > 36) & (df.Lat < 70) &
                   (df.Long > -26) & (df.Long < 34))
    # print(f"Applied Positional and European filters: remains {df.count()} records")

    df = df.filter(~df.Mlat)
    # print(f"Applied GPS Filter: remains {df.count()} records")
    return df
    pass


def group_icao(df: DataFrame, cols, cols_names):
    # df = df.orderBy(df.PosTime)  # useless, order is not kept with groupBy
    df = df.select(df.Icao, *cols)
    df_group = df.groupBy(df.Icao)\
                 .agg(*[F.collect_list(df[c]).alias(c_name) for c, c_name in zip(cols, cols_names)])
    # df_group = df_group.select('*', F.size(cols[0]).alias("Size"))
    
    # print(f"Groupes by ID: remains {df_group.count()} records")

    # df_group = df_group.orderBy(df_group["Size"].desc())
    return df_group


def sort_by_time(rdd: RDD):
    def map_sort_time(record):
        alt, lat, long, speed, time = record.Alt, record.Lat, record.Long, record.Speed, record.Time
        # Magic sort in python
        assert(len(time) == len(lat) == len(long) == len(speed) == len(alt))
        if len(time) > 0:
            time, alt, lat, long, speed = map(list, zip(*sorted(zip(time, alt, lat, long, speed))))
        assert(sorted(time) == time)
        return Row(Icao=record.Icao,
                   Lat=lat,
                   Long=long,
                   Alt=alt,
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
                  sticky: float=float("inf"),
                  time_threshold: float=float("inf")) -> RDD:
    def flatmap_split_trips(record, 
                            airport: AirportFinder=airport,
                            air_info=air_info,
                            sticky=sticky,
                            time_threshold=time_threshold):
        """ Split the record for each plane in multiple records:
        One for each trip the plane took.
        """
        alt, lat, long, speed, time = record.Alt, record.Lat, record.Long, record.Speed, record.Time
        # Ideas: - Apply low frequency filter on the altitude to
        #          remove noise from data ?
        #        - Do the same on Speed values ?

        # Get local minimums of the altitude -> this might be take off and landing

        if len(time) <= 2:
            return []

        local_alt_bool = np.array([a < local_alt for a, local_alt in zip(alt,
                                                                         [airport.local_max_alt(pos, air_info) for pos in zip(lat, long)])])                                                 

        split_indices = [i for i, (a, b) in enumerate(zip(np.gradient(np.sign(np.gradient(alt))) > 0,
                                                          local_alt_bool))
                         if a and b]

        # TODO: Here compute distance airport - position and validate the landing.
        # TODO: if too big time shifts appears -> filter out ?
        #                                      -> land and cut ?

        corresponding_airports = [airport.closest_airport((lat[i], long[i]))
                                  for i in split_indices]

        couples = [(i, j) for i, j in zip(split_indices[:-1], split_indices[1:])]

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

            row = Row(Icao=record.Icao,
                      From=air_from,
                      To=air_to,
                      Lat=lat[i:j+1],
                      Long=long[i:j+1],
                      Alt=alt[i:j+1],
                      Speed=speed[i:j+1],
                      Time=time[i:j+1],
                      )
            trip_l.append(row)
        return trip_l

    # rdd = sc.parallelize(rdd.take(1))
    # print(rdd.count())
    # print(rdd.take(1))
    rdd = rdd.flatMap(flatmap_split_trips)
    # print(f"Applied trips recognition, remains {rdd.count()} records")
    return rdd


def get_clean_data(file) -> RDD:
    air_dict, air_loc, air_finder = get_european_airports()  # All info about airports
    df = filter_data(file)  # European filter

    df_grouped = group_icao(df,
                            labels,
                            labels_names)  # Group by ICAO

    rdd = sort_by_time(df_grouped.rdd)  # Return a rdd where each record is a Row

    # rdd = filter_big_time_step(rdd, -1, float("inf"))  # Filter out ill formed time steps

    rdd = flatten_trips(rdd, air_finder, air_dict, time_threshold=15*60*1000)
    return rdd


def draw_records(rdd: RDD, n: int):

    m = Basemap(llcrnrlon=-30., llcrnrlat=30., urcrnrlon=40., urcrnrlat=75.,
                rsphere=(6378137.00, 6356752.3142),
                resolution='l', projection='merc',
                lat_ts=20.)

    colors = itertools.cycle("bgrcmy")
    for c, record in zip(colors, rdd.take(n)):
        print("*" * 20)
        lat, long, time, alt, speed = record.Lat, record.Long, record.Time, record.Alt, record.Speed
        print((record["From"]["Lat"],  record["From"]["Long"], record["From"]["Alt"]))
        m.drawgreatcircle(record["From"]["Long"], record["From"]["Lat"], long[0], lat[0], linewidth=1, color="k")

        for x1, x2 in zip(zip(lat[:-1], long[:-1], time[:-1], alt[:-1], speed[:-1]),
                          zip(lat[1:], long[1:], time[1:], alt[1:], speed[1:])):
            lat1, long1, t1, alt1, s1 = x1
            lat2, long2, t2, alt2, s2 = x2
            print((lat1, long1, alt1, s1), (lat2, long2, alt2, s2), (t2-t1)/1000)
            m.drawgreatcircle(long1, lat1, long2, lat2, linewidth=1, color=c)

        print((record["To"]["Lat"], record["To"]["Long"], record["To"]["Alt"]))
    m.drawgreatcircle(long[-1], lat[-1], record["To"]["Long"], record["To"]["Lat"], linewidth=1, color="k")
    m.drawcoastlines()
    m.fillcontinents()
    # draw parallels
    m.drawparallels(np.arange(10, 90, 5), labels=[1, 1, 0, 1])
    # draw meridians
    m.drawmeridians(np.arange(-180, 180, 10), labels=[1, 1, 0, 1])
    plt.show()


def main():
    file_json = "./data/2017-01-01/2017-01-01-*.json"
    rdd = get_clean_data(file_json)

    print(rdd.take(20))

    draw_records(rdd, 3)

    """
    record, min_step, max_step = rdd.take(1)[0]
    print(record.Icao)

    lat, long, time, alt, speed = record.Lat, record.Long, record.Time, record.Alt, record.Speed

    m = Basemap(llcrnrlon=-30., llcrnrlat=30., urcrnrlon=40., urcrnrlat=75.,
                rsphere=(6378137.00, 6356752.3142),
                resolution='l', projection='merc',
                lat_0=np.mean(lat), lon_0=np.mean(long), lat_ts=20.)

    print(time[0])
    for x1, x2 in zip(zip(lat[:-1], long[:-1], time[:-1], alt[:-1], speed[:-1]), 
                      zip(lat[1:], long[1:], time[1:], alt[1:], speed[1:])):
        lat1, long1, t1, alt1, s1 = x1
        lat2, long2, t2, alt2, s2 = x2
        print((lat1, long1, alt1, s1), (lat2, long2, alt2, s2), (t2-t1)/1000)
        m.drawgreatcircle(long1, lat1, long2, lat2, linewidth=1, color='b')
    print(time[-1])
    m.drawcoastlines()
    m.fillcontinents()
    # draw parallels
    m.drawparallels(np.arange(10, 90, 5), labels=[1, 1, 0, 1])
    # draw meridians
    m.drawmeridians(np.arange(-180, 180, 10), labels=[1, 1, 0, 1])
    plt.show()
    """
    pass


if __name__ == '__main__':

    main()
