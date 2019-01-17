import numpy as np
from matplotlib import pyplot as plt
from haversine import haversine
from scipy.special import factorial

from utils import vectorized_haversine
from process_trips import get_trips, read_files
from plotting_data import draw_records


def histogram(trip_l):
    real_d = [sum(t['Dist']) for t in trip_l]
    perfect_d = vectorized_haversine(np.array([[t['From']['Lat'], t['From']['Long']] for t in trip_l]),
                                     np.array([[t['To']['Lat'], t['To']['Long']] for t in trip_l]))
    efficiency = [real / perfect - 1 for real, perfect in zip(real_d, perfect_d)]

    print(*list(zip(real_d, perfect_d, efficiency)), sep='\n')

    mean_eff = np.mean(efficiency)
    var_eff = np.var(efficiency)
    print(mean_eff, var_eff)
    theta = var_eff / mean_eff
    k = mean_eff**2 / var_eff
    x = np.linspace(0.00001, 2, 10000)
    gamma = np.power(k-1, x) * np.exp(-x/theta)/(factorial(k-1)*theta**k)

    plt.hist(efficiency, bins=300, density=True, range=(0, 2))
    plt.plot(x, gamma, 'r--')
    plt.show()


def main():
    rdd = read_files("data/processed/part-*")
    trip_l = rdd.collect()
    histogram(trip_l)

if __name__ == '__main__':
    main()
