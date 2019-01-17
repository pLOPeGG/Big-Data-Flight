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


def main():
    rdd = read_files("file:///home/s1493299/save_jan/part-*")
    rdd = filter_missing_points(rdd)
    rdd = rdd.map(lambda x: ((x.From['Code'], x.To['Code']), [(lat, long) for lat, long in zip(x.Lat, x.Long)]))
    rdd = rdd.map(lambda x: (x[0], {sum([haversine(old, new) for old, new in zip(x[1][:-1], x[1][1:])]) /
                             haversine(x[1][0], x[1][-1])}))
    rdd = rdd.reduceByKey(lambda x, y: set.union(x, y))  # use groupby depending on final use
    print(rdd.take(10))
    pass


if __name__ == '__main__':
    main()
