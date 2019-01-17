from pyspark import SparkContext
import numpy as np
from pyspark.sql import Row
from haversine import haversine

sc = SparkContext()
sc.addPyFile('src/utils.py')
sc.addPyFile('src/airports.py')


def read_files(path):
    # ugliest hack ever
    def row_repr(record, Row=Row):
        # Some NAN are inside the record
        record = str(record).replace('nan', '0000')
        loc = {'Row': Row}
        exec('r = ' + record, globals(), loc)
        return loc['r']

    rdd = sc.textFile(path).map(row_repr)
    return rdd


def filter_missing_points(rdd):
    threshold = 1000 * 1000  # 1000 seconds MAXIMUM
    return rdd.filter(lambda r, t=threshold: all(np.diff(r.Time) < t))


def flight_efficiency(rdd):
    """ Flight efficiency as traveled distance divided by optimal distance """
    rdd = rdd.map(lambda x: ((x.From['Code'], x.To['Code'], x.Op), [(lat, long) for lat, long in zip(x.Lat, x.Long)]))\
        .map(lambda x: (x[0], sum([haversine(old, new) for old, new in zip(x[1][:-1], x[1][1:])]) /
                        haversine(x[1][0], x[1][-1])))
    return rdd


def flight_efficiency_per_company(rdd):
    rdd_by_op = flight_efficiency(rdd)\
        .map(lambda x: (x[0][2], (x[1], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1])) \
        .sortBy(lambda x: x[2], ascending=False)
    return rdd_by_op


def flight_efficiency_per_route(rdd):
    rdd_by_route = flight_efficiency(rdd)\
        .map(lambda x: (x[0][0:2], (x[1], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1]))
    return rdd_by_route


def main():
    rdd = read_files("file:///home/s1493299/save_jan/part-*")
    rdd = filter_missing_points(rdd)

    print(flight_efficiency_per_company(rdd).take(20))
    print(flight_efficiency_per_route(rdd).take(200))


if __name__ == '__main__':
    main()
