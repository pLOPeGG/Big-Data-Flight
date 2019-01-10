from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.basemap import Basemap





def main():
    # plt.figure(figsize=(8, 8))
    # m = Basemap(projection='ortho', resolution=None, lat_0=45, lon_0=0)
    # m.bluemarble(scale=0.5)
    # plt.show()

    sc = SparkContext()
    spark = SparkSession(sc)
    base_df = spark.read.json("./data/2017-01-01/2017-01-01-1{2,3}*Z.json")
    df = base_df.filter(base_df.Lat.isNotNull() & base_df.Long.isNotNull())
    df = df.filter((df.Lat > 36) & (df.Lat < 70) &
                   (df.Long > -26) & (df.Long < 34))

    df_reduce = df.select(df.Id, df.Lat, df.Long, df.PosTime, df.Alt, df.Gnd, df.Cos)
    df_reduce = df_reduce.groupBy(df_reduce.Id).agg(F.collect_list(df.Lat).alias("Lat"),
                                                    F.collect_list(
                                                        df.Long).alias("Long"),
                                                    F.collect_list(df.PosTime).alias("Time"),
                                                    F.collect_list(
                                                        df.Alt).alias("Alt"),
                                                    F.collect_list(
                                                        df.Gnd).alias("Gnd"),
                                                    F.collect_list(df.Cos).alias("Cos"))

    print(base_df.count())
    print(df.count())
    print(df_reduce.count())

    print(df_reduce.take(1))
    record = df_reduce.take(1)[0]
    lat, long, time = record.Lat, record.Long, record.Time

    _, lat, long = zip(*sorted(zip(time, lat, long)))

    m = Basemap(llcrnrlon=-30., llcrnrlat=30., urcrnrlon=40., urcrnrlat=75.,
                rsphere=(6378137.00, 6356752.3142),
                resolution='l', projection='merc',
                lat_0=np.mean(lat), lon_0=np.mean(long), lat_ts=20.)

    for x1, x2 in zip(list(zip(lat[:-1], long[:-1])), list(zip(lat[1:], long[1:]))):
        lat1, long1 = x1
        lat2, long2 = x2
        print(x1, x2)
        m.drawgreatcircle(long1, lat1, long2, lat2, linewidth=1, color='b')

    m.drawcoastlines()
    m.fillcontinents()
    # draw parallels
    m.drawparallels(np.arange(10, 90, 5), labels=[1, 1, 0, 1])
    # draw meridians
    m.drawmeridians(np.arange(-180, 180, 10), labels=[1, 1, 0, 1])
    plt.show()

    pass


if __name__ == '__main__':
    main()
