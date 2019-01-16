# from mpl_toolkits.basemap import Basemap
# import matplotlib.pyplot as plt

# def draw_records(rdd: RDD, n: int):
# 
#     m = Basemap(llcrnrlon=-30., llcrnrlat=30., urcrnrlon=40., urcrnrlat=75.,
#                 rsphere=(6378137.00, 6356752.3142),
#                 resolution='l', projection='merc',
#                 lat_ts=20.)

#     colors = itertools.cycle("bgrcmy")
#     for c, record in zip(colors, rdd.take(n)):
#         print("*" * 20)
#         lat, long, time, alt, speed = record.Lat, record.Long, record.Time, record.Alt, record.Speed
#         print((record["From"]["Lat"],  record["From"]["Long"], record["From"]["Alt"]))
#         m.drawgreatcircle(record["From"]["Long"], record["From"]["Lat"], long[0], lat[0], linewidth=1, color="k")

#         for x1, x2 in zip(zip(lat[:-1], long[:-1], time[:-1], alt[:-1], speed[:-1]),
#                           zip(lat[1:], long[1:], time[1:], alt[1:], speed[1:])):
#             lat1, long1, t1, alt1, s1 = x1
#             lat2, long2, t2, alt2, s2 = x2
#             print((lat1, long1, alt1, s1), (lat2, long2, alt2, s2), (t2-t1)/1000)
#             m.drawgreatcircle(long1, lat1, long2, lat2, linewidth=1, color=c)

#         print((record["To"]["Lat"], record["To"]["Long"], record["To"]["Alt"]))
#     m.drawgreatcircle(long[-1], lat[-1], record["To"]["Long"], record["To"]["Lat"], linewidth=1, color="k")
#     m.drawcoastlines()
#     m.fillcontinents()
#     # draw parallels
#     m.drawparallels(np.arange(10, 90, 5), labels=[1, 1, 0, 1])
#     # draw meridians
#     m.drawmeridians(np.arange(-180, 180, 10), labels=[1, 1, 0, 1])
#     plt.show()