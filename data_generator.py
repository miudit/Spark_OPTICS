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

    file_write = open("data_2M_4d.csv", "w")

    """
    for i in range(8):
        cluster = np.random.normal(20*i+10, 3, (20, 2))
        #data += cluster
        for data in cluster:
            data = list(data)
            file_write.write("{0:.3f}".format(data[0]) + ',' + "{0:.3f}".format(data[1]) + '\n')
    """

    """
    for i in range(8):
        attributes1 = list(np.random.normal(20*i+10, 1, 20))
        attributes2 = list(np.random.normal(10, 1, 20))
        for (a, b) in zip(attributes1, attributes2):
            file_write.write("{0:.3f}".format(a) + ',' + "{0:.3f}".format(b) + '\n')
    """

    num_of_attributes = 4
    num_of_class = 64
    data_size = 20000
    size_per_class = data_size / num_of_class
    centers = np.random.rand(num_of_class, num_of_attributes) * 800 + 100
    variation = 10

    for i in range(num_of_class):
        center = list(centers[i])
        attributes_list = list()
        for j in range(num_of_attributes):
            attributes = np.random.normal(center[j], variation, size_per_class)
            attributes_string = list(map(str, attributes))
            attributes_list.append( attributes_string )
        for data in list(map(list, zip(*attributes_list))):
            #print(",".join(data))
            file_write.write(",".join(data) + '\n')
        #for s in list(map(lambda x: ",".join(list(x)), attributes_list)):
        #    print(s)
        #for (a, b) in zip(attributes1, attributes2):
        #    file_write.write("{0:.3f}".format(a) + ',' + "{0:.3f}".format(b) + '\n')
