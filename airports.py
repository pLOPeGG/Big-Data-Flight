import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from typing import Dict, Tuple


def get_airports(file) -> pd.DataFrame:
    air_loc = pd.read_csv(file)
    air_loc = air_loc[(air_loc['Lat'] > 36) & (air_loc['Lat'] < 70) &
                      (air_loc['Long'] > -26) & (air_loc['Long'] < 34)]
    return air_loc


def compute_KD(points) -> cKDTree:
    return cKDTree(points)


def get_european_airports() -> Tuple[Dict[Tuple[float, float], str], cKDTree]:
    air_loc = get_airports("data/gadb_country_declatlon.csv")
    air_dict = {(i[1], i[2]): i[0] for i in air_loc.values}
    tree = compute_KD(air_loc.values[:, 1:])
    return air_dict, tree


def main():
    air_loc = get_airports("data/gadb_country_declatlon.csv")
    air_dict = {(i[1], i[2]): i[0] for i in air_loc.values}
    # print(air_dict)

    print(air_loc.values[:, 1:])
    tree = compute_KD(air_loc.values[:, 1:])
    
    dis, index = tree.query([[45.75, 5.08]])
    print(tuple(tree.data[index[0]]), air_dict[tuple(tree.data[index[0]])])
    pass


if __name__ == '__main__':
    main()
