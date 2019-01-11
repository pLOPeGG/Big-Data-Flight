import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from typing import Dict, Tuple
from haversine import haversine
import time

# Data for the airports can be found at "http://www.partow.net/downloads/GlobalAirportDatabase_CountryDecLatLon.zip"
# and extract it in the data directory (csv file)
# Append a header line to the csv for columns titles:
# COUNTRY,Lat,Long


class AirportFinder():
    """Finds closest airport quickly using numpy vectorization
    Performances are close to what a KDTree provides when using 1000 airports
    Might not scale well with larger numbers!
    """

    def __init__(self, airport_l: np.ndarray):
        super().__init__()
        self.deg_airport_l = airport_l
        self.airport_l = np.deg2rad(airport_l)

    def closest_airport(self, point: np.ndarray) -> np.ndarray:
        """ Finds the closest airport to the point
        @param point: array containing latitude and longitude in degree
        @return: closest airport coordinates in degree
        """
        rad = np.deg2rad(point)

        lat = rad[0] - self.airport_l[:, 0]
        lng = rad[1] - self.airport_l[:, 1]

        add0 = np.cos(self.airport_l[:, 0]) * \
            np.cos(rad[0]) * np.sin(lng * 0.5) ** 2
        d = np.sin(lat * 0.5) ** 2 + add0

        return self.deg_airport_l[np.argmin(d)]


def get_airports(file) -> pd.DataFrame:
    air_info = pd.read_csv(file)
    return air_info


def get_european_airports() -> Tuple[Dict[Tuple[float, float], str],
                                     np.ndarray,
                                     AirportFinder]:
    air_info = get_airports("./data/gadb_country_declatlon.csv")
    air_info = air_info[(air_info['Lat'] > 36) & (air_info['Lat'] < 70) &
                        (air_info['Long'] > -26) & (air_info['Long'] < 34)]
    air_dict = {(i[1], i[2]): i[0] for i in air_info.values}
    air_loc = np.array(air_info.values[:, 1:], dtype='float64')
    air_finder = AirportFinder(air_loc)
    return air_dict, air_loc, air_finder


def compute_KD(points) -> cKDTree:
    """ DEPRECATED USE AIRFINDER FOR ACCURACY	
    Beware, this is a KDTree working with Euclidean distance. 
    This is not perfect for Spherical surface and might give wrong answers (?)
    Yes it does -> any other solution ? 
    """
    return cKDTree(points)


def benchmark():
    air_dict, air_loc, air_finder = get_european_airports()

    tree = compute_KD(air_loc)

    beg = time.clock()
    for x in np.linspace(40, 70, 100):
        for y in np.linspace(-25, 40, 100):
            point = [x, y]
            af_airport = air_finder.closest_airport(point)
    print(f"AirportFinder took {time.clock()-beg}sec to run")

    beg = time.clock()
    for x in np.linspace(40, 70, 100):
        for y in np.linspace(-25, 40, 100):
            point = [x, y]
            haversine_airport = air_loc[np.argmin(
                [haversine(point, p) for p in air_loc])]
    print(f"Haversine function took {time.clock()-beg}sec to run")

    beg = time.clock()
    for x in np.linspace(40, 70, 100):
        for y in np.linspace(-25, 40, 100):
            point = [x, y]
            _, index = tree.query([point])
            tree_airport = tree.data[index[0]]
    print(f"cDKTree took {time.clock()-beg}sec to run")
    pass


def main():
    air_dict, air_loc, air_finder = get_european_airports()

    point = [37.478439, -6.047272]
    print(air_finder.closest_airport(point))
    pass


if __name__ == '__main__':
    main()
    # benchmark()