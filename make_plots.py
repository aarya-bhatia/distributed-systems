import matplotlib.pyplot as plt
import numpy as np

with open("output", "r") as file:
    lines = file.readlines()

    y = []
    e = []
    samples = []

    for line in lines:
        values = np.array(list(map(np.float64, line.split())))
        print(values)
        if values.size > 0:
            average = np.mean(values)
            std = np.std(values)
            y.append(average)
            e.append(std)
            samples.append(values.size)

    y = np.array(y)
    e = np.array(e)
    x = np.arange(1, y.size + 1)


# x = np.array([1, 2, 3, 4, 5])
# y = np.power(x, 2)  # Effectively y = x**2
# e = np.array([1.5, 2.6, 3.7, 4.6, 5.5])

plt.plot(x, np.array(samples))

plt.errorbar(x, y, e, linestyle='None', marker='^')
plt.show()
