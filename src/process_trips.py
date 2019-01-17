from pyspark import SparkContext
import numpy as np
from pyspark.sql import Row
import plotting_data

from utils import vectorized_haversine
from airports import get_european_airports

sc = SparkContext()
sc.addPyFile('src/utils.py')
sc.addPyFile('src/airports.py')

def read_files(path):
    # ugliest hack ever
    def row_repr(record, Row=Row):
        # Some NAN are inside the record
        record = str(record).replace('nan', '0000')

        loc = {'Row': Row, 'array': np.array}
        exec('r = ' + record, globals(), loc)
        return loc['r']

    rdd = sc.textFile(path).map(row_repr)
    return rdd


def pre_process_trips(rdd,
                      airport,
                      air_info):
    """ This function is useless as most of the new trips are nonsense.
    Use filter_missing_points instead that provide good quality trips only.
    """
    thre = 2000 * 1000  # 2000 sec

    def split_trips(record,
                    airport=airport,
                    air_info=air_info,
                    threshold=thre):
        time, lat, long = np.array(record.Time), np.array(record.Lat), np.array(record.Long)
        split_b = np.concatenate(([False], np.diff(time) > thre))
        split_i = [i for i, v in enumerate(split_b) if v]

        couples = []

        if len(split_i) > 1:
            couples = [(i, j) for i, j in zip(split_i[:-1], split_i[1:])]
            couples = [(0, split_i[0])] + couples[:] + [(split_i[-1], len(time))]
        elif len(split_i) == 1:
            couples = [(0, split_i[0]), (split_i[0], len(time))]
        else:
            # Add Mod attribute to record and return
            temp = record.asDict()
            temp["Mod"] = False
            record = Row(**temp)
            return [record]
        record_lst = []
        for i, j in couples:
            if i+1 == j:
                continue
            air_from = air_info[airport.closest_airport((lat[i], long[i]))]
            air_to = air_info[airport.closest_airport((lat[j-1], long[j-1]))]

            if air_from == air_to:
                continue
       
            row = Row(Icao=record.Icao,
                      Op=record.Op,
                      Engines=record.Engines,
                      Mil=record.Mil,
                      Cou=record.Cou,
                      Mdl=record.Mdl,
                      From=air_from,
                      To=air_to,
                      Lat=lat[i:j],
                      Long=long[i:j],
                      Alt=record.Alt[i:j],
                      TargetAlt=record.TargetAlt[i:j],
                      Speed=record.Speed[i:j],
                      Time=time[i:j],
                      Mod=True
                      )
            record_lst.append(row)
        return record_lst

    return rdd.flatMap(split_trips)
    pass


def filter_missing_points(rdd):
    threshold = 1000 * 1000  # 1000 seconds MAXIMUM
    return rdd.filter(lambda r, t=threshold: all(np.diff(r.Time) < t))


def trip_len(rdd):
    def map_trip_len(record):
        lat, long = record.Lat, record.Long
        from_airport, to_airport = record.From, record.To
        lat = [from_airport['Lat']] + lat + [to_airport['Lat']]
        long = [from_airport['Long']] + long + [to_airport['Long']]
        pos = np.concatenate((lat, long)).reshape(2, -1).T
        dist = vectorized_haversine(pos[:-1, :], pos[1:, :]).tolist()

        temp = record.asDict()
        temp["Dist"] = dist
        record = Row(**temp)

        return record
    return rdd.map(map_trip_len)


def get_trips(file):
    air_dict, air_loc, air_finder = get_european_airports()  # All info about airports
    rdd = read_files(file)
    # print(rdd.count())  # -> 35000 trips

    # rdd = pre_process_trips(rdd, air_finder, air_dict)  # -> 54000 trips
    # print(rdd.filter(lambda r: not r.Mod).count())  # -> 6400 trips (thre = 2000sec)

    rdd = filter_missing_points(rdd)
    # print(rdd.count())  # -> 5625 trips (thre = 1000sec)

    rdd = trip_len(rdd)
    return rdd


def main():

    # rdd = sc.textFile("data/processed/part-*")
    # print(rdd.take(1))

    seed = 0
    files = "data/save_jan/part-*"
    rdd = get_trips(files)

    rdd.saveAsTextFile("data/processed")

    # plotting_data.draw_records(rdd, 500, colors="k", alpha=0.1, seed=seed)
    pass


if __name__ == '__main__':
    main()
