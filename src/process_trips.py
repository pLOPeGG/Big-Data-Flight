from pyspark import SparkContext
import numpy as np
from pyspark.sql import Row
import plotting_data

from airports import get_european_airports

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


def pre_process_trips(rdd,
                      airport,
                      air_info):
                      
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


def main():
    air_dict, air_loc, air_finder = get_european_airports()  # All info about airports
    rdd = read_files("data/save_jan/part-*")
    rdd = pre_process_trips(rdd, air_finder, air_dict)

    print(rdd.count())
    print(rdd.filter(lambda r: not r.Mod).count())
    
    plotting_data.draw_records(rdd, -1, colors="k", alpha=0.4)
    pass


if __name__ == '__main__':
    main()
