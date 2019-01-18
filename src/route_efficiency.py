from pyspark import SparkContext
import numpy as np
from pyspark.sql import Row
from haversine import haversine
from matplotlib import pyplot as plt

sc = SparkContext()


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


def filter_short_routes(rdd):
    """ filter out routes shorter than 100 km, they mostly go from a city towards the same city"""
    return rdd.filter(lambda x: haversine((x.From['Lat'], x.From['Long']), (x.To['Lat'], x.To['Long'])) > 100)


def flight_efficiency(rdd):
    """ Flight efficiency as traveled distance divided by optimal distance """
    rdd = rdd.map(lambda x: (((x.From['Lat'], x.From['Long']), (x.To['Lat'], x.To['Long']), x.Op), [(lat, long) for lat, long in zip(x.Lat, x.Long)]))\
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
        .map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1])) \
        .sortBy(lambda x: x[2], ascending=False)
    return rdd_by_route


def flight_efficiency_per_route_per_company(rdd):
    rdd = flight_efficiency(rdd) \
        .map(lambda x: (x[0], (x[1], 1))) \
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda x: (x[0][:2], [(x[0][2], x[1][0] / x[1][1])]))\
        .reduceByKey(lambda x, y: x+y)\
        .map(lambda x: sorted(x[1], key=lambda y: y[1])) \
        .flatMap(lambda x: [(val[0].lower(), (val[1] / (sum(e[1] for e in x)/len(x)), 1)) for val in x])\
        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
        .map(lambda x: (x[0], x[1][0]/x[1][1]))\
        .sortBy(lambda x: x[1])

    return rdd


def plot_records(rdd):
    result = rdd.filter(lambda x: x[1] < 2).map(lambda x: x[1:]).collect()
    flights, efficiency = zip(*result)
    plt.plot(flights, efficiency, 'o')
    plt.show()



def main():
    rdd = read_files("./data/save_jan/part-*")
    rdd = filter_short_routes(filter_missing_points(rdd))

    # plot_records(flight_efficiency_per_company(rdd))

    comp = flight_efficiency_per_company(rdd).take(20)
    comps = {x[0].lower() for x in comp}

    records = flight_efficiency_per_route_per_company(rdd) \
        .filter(lambda x: x[0] in comps) \
        .collect()
    for rec in comp:
        print(rec)
    for rec in records:
        print(rec)


if __name__ == '__main__':
    main()
