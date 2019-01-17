
import numpy as np

AVG_EARTH_RADIUS = 6_365_632.  # At latitude 50°


def vectorized_haversine(from_l: np.ndarray, to_l: np.ndarray) -> np.ndarray:
    """ Code from StackOverflow: 
    https://stackoverflow.com/questions/44681828/efficient-computation-of-minimum-of-haversine-distances
    """
    f = np.deg2rad(from_l)
    t = np.deg2rad(to_l)

    lat = f[:, 0] - t[:, 0]
    lng = f[:, 1] - t[:, 1]

    add0 = np.cos(t[:, 0]) * np.cos(f[:, 0]) * np.sin(lng * 0.5) ** 2
    d = np.sin(lat * 0.5) ** 2 + add0

    return 2 * AVG_EARTH_RADIUS * np.arcsin(np.sqrt(d))


def main():
    print(vectorized_haversine(np.array([[47.461349, 8.480347], [47.427047, 8.406231]]),
                               np.array([[47.427047, 8.406231], [47.414154, 8.287537]])))
    pass


if __name__ == '__main__':
    main()
