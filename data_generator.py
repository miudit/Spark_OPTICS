# -*- coding: utf-8 -*-
import sys
import glob
import unittest
import random
import csv
import itertools
import collections
import json
import numpy as np

if __name__ == "__main__":

    file_write = open("data.csv", "w")

    """
    for i in range(8):
        cluster = np.random.normal(20*i+10, 3, (20, 2))
        #data += cluster
        for data in cluster:
            data = list(data)
            file_write.write("{0:.3f}".format(data[0]) + ',' + "{0:.3f}".format(data[1]) + '\n')
    """

    for i in range(8):
        attributes1 = list(np.random.normal(20*i+10, 1, 20))
        attributes2 = list(np.random.normal(10, 1, 20))
        for (a, b) in zip(attributes1, attributes2):
            file_write.write("{0:.3f}".format(a) + ',' + "{0:.3f}".format(b) + '\n')
