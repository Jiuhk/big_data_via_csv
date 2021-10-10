import csv
import os
import pandas as pd
import numpy as np
import time
import math
from multiprocessing import Process


# create folder to store csv
if not os.path.exists('csv_files'):
    os.mkdir('csv_files')


# for file names of each date in the required period
times = pd.date_range(start='09/01/2020', end='08/31/2021', freq='D')


def make_csv(csv_file_list):
    for csv_file_name in (['csv_files\\' + str(i.date()) + '.csv' for i in csv_file_list]):

        # generate 1,000,000 random time & sort
        hh = np.random.randint(0,24,1000000)
        mm = np.random.randint(0,60,1000000)
        ss = np.random.randint(0,60,1000000)
        random_time_zipped = zip(hh, mm, ss)
        random_time_zipped = sorted(random_time_zipped)
        random_time = [str(i) + ':' +str(j) + ':' + str(k)  for i, j, k in random_time_zipped]
        if not os.path.exists('csv_files\\' + csv_file_name[10:18]):
            os.mkdir('csv_files\\' + csv_file_name[10:18])

        csv_file_name = 'csv_files\\' + csv_file_name[10:18] + '\\' + csv_file_name[10:]
        with open(csv_file_name, 'w', newline='') as csv_file:
            filewriter = csv.writer(csv_file, delimiter=',')
            filewriter.writerow(['ID','time','customerID','serviceID','country','usage'])
            ID = 1

            # generate 1,000,000 rows
            for i in range(1000000):
                customerID = np.random.randint(1,1000)
                country = ['CN', 'UK', 'US', 'FR', 'DE', 'ES', 'CA', 'IN', 'JP', 'KR', 'SG']
                usage = math.floor(np.random.random()*1000000000)
                serviceID_sub = np.random.randint(1,100)
                if serviceID_sub < 10:
                    serviceID_sub = '00' + str(serviceID_sub)
                elif serviceID_sub >= 10:
                    serviceID_sub = '0' + str(serviceID_sub)
                filewriter.writerow([ID, random_time[i], customerID, str(customerID) + serviceID_sub, country[np.random.randint(0,11)], usage])
                ID += 1


        print(csv_file_name + ' done')


if __name__ == '__main__':
    processes = []
    split = np.array_split(times, os.cpu_count())
    for k, i in enumerate(split):
        print('registration process ' + str(k))
        processes.append(Process(target=make_csv, args=(list(i),)))

    for process in processes:
        process.start()

    for process in processes:
        process.join()

    
