from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession, DataFrame, functions as F
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.basemap import Basemap

from airports import AirportFinder


sc = SparkContext()
spark = SparkSession(sc)

labels = ["Lat", "Long", "PosTime", "GAlt", "Spd"]
labels_names = ["Lat", "Long", "Time", "Alt", "Speed"]


def filter_data(input_json: str):
    base_df = spark.read.json(input_json)
    print(f"Loaded {input_json}: {base_df.count()} records")
    df = base_df.filter(base_df.Lat.isNotNull() & base_df.Long.isNotNull())
    df = df.filter((df.Lat > 36) & (df.Lat < 70) &
                   (df.Long > -26) & (df.Long < 34))
    print(
        f"Applied Positional and European filters: remains {df.count()} records")

    df = df.filter(~df.Mlat)
    print(f"Applied GPS Filter: remains {df.count()} records")
    return df
    pass


def group_icao(df: DataFrame, cols, cols_names):
    df = df.orderBy(df.PosTime)
    df = df.select(df.Icao, *cols)
    df_group = df.groupBy(df.Icao)\
                 .agg(*[F.collect_list(df[c]).alias(c_name) for c, c_name in zip(cols, cols_names)])
    df_group = df_group.select('*', F.size(cols[0]).alias("Size"))
    print(f"Groupes by ID: remains {df_group.count()} records")
    df_group = df_group.orderBy(df_group["Size"].desc())
    return df_group


def filter_big_time_step(rdd: RDD, min_step: int, max_step: int) -> RDD:
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

    print(
        f"Applied filter on time steps of size {(min_step, max_step)}, remains {rdd.count()} records")
    return rdd
    pass


def flatten_trips(rdd: RDD, airport: AirportFinder, sticky=float("inf"): float) -> RDD:
    def map_split_trips(record):
        """ Split the record for each plane in multiple records:
        One for each trip the plane took.
        """
        alt = record.Alt

        # Ideas: - Apply low frequency filter on the altitude to
        #          remove noise from data ?
        #        - Do the same on Speed values ?
        #        - 

        # Get local minimums of the altitude -> this might be take off and landing
        split_indices = np.gradient(np.sign(np.gradient(alt))) > 0 \
            and np.array(alt) < airport.max_height

        split_indices = [i for i in split_indices if airport.closest_airport(i) < sticky]

        couples = [(i, j) for i, j in zip(alt[:-1], alt[1:])]


        pass

    pass


def get_clean_data(file) -> RDD:
    df = filter_data(file)  # European filter

    df_sorted = group_icao(df,
                           labels,
                           labels_names)  # Group by ICAO and sort time

    rdd = df_sorted.rdd
    # rdd = filter_big_time_step(rdd, 0, 500000)  # Filter out ill formed time steps
    return rdd


def draw_records(rdd: RDD, n: int):

    m = Basemap(llcrnrlon=-30., llcrnrlat=30., urcrnrlon=40., urcrnrlat=75.,
                rsphere=(6378137.00, 6356752.3142),
                resolution='l', projection='merc',
                lat_ts=20.)

    for record in rdd.take(n):
        lat, long, time, alt, speed = record.Lat, record.Long, record.Time, record.Alt, record.Speed
        for x1, x2 in zip(zip(lat[:-1], long[:-1], time[:-1], alt[:-1], speed[:-1]),
                          zip(lat[1:], long[1:], time[1:], alt[1:], speed[1:])):
            lat1, long1, t1, alt1, s1 = x1
            lat2, long2, t2, alt2, s2 = x2
            print((lat1, long1, alt1, s1), (lat2, long2, alt2, s2), (t2-t1)/1000)
            m.drawgreatcircle(long1, lat1, long2, lat2, linewidth=1, color='b')
    m.drawcoastlines()
    m.fillcontinents()
    # draw parallels
    m.drawparallels(np.arange(10, 90, 5), labels=[1, 1, 0, 1])
    # draw meridians
    m.drawmeridians(np.arange(-180, 180, 10), labels=[1, 1, 0, 1])
    plt.show()


def main():
    file_json = "./data/2017-01-01/2017-01-01-*.json"
    df = filter_data(file_json)

    df_sorted = group_icao(df,
                           labels,
                           labels_names)

    rdd = df_sorted.rdd

    rdd = filter_big_time_step(rdd, 0, 1000000)

    draw_records(rdd, 10)

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
