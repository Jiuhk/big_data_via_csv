from collections import defaultdict
import pandas as pd
import csv
from os import cpu_count, listdir, read
from os.path import isfile, join
from multiprocessing import Process, Pool
import numpy as np
import time


csv_files_dir_path = 'csv_files\\'
csv_files_monthly_path = listdir(csv_files_dir_path)


def read_csvs(path):

    pd_months = []
    for csv_monthly in path:
        master_list = []
        pd_month = pd.DataFrame(master_list, columns=['ID','time','customerID','serviceID','country','usage'])
        print('Analyzing ' + csv_monthly)

        for k, csv_file_path in enumerate([csv_files_dir_path + csv_monthly + '\\' + f for f in listdir(csv_files_dir_path + csv_monthly)]):
            df = pd.read_csv(csv_file_path)
            pd_month = pd.concat([pd_month, df])
            if k == 3: # scale reduced
                break
        pd_months.append({csv_monthly: pd_month})

    return pd_months


if __name__ == '__main__':
    processes = []
    with Pool(processes=cpu_count()) as pool:
        split = np.array_split(csv_files_monthly_path, cpu_count())
        list_monthly_reports = pool.map(read_csvs, split)
        monthly_reports = {}

        for process in list_monthly_reports:
            for month in process:
                for k, v in month.items():
                    monthly_reports[k] = v

    while True:
        report = input("report 1, 2 or 3? ")
        
        if report == '1':
            report1 = {}
            for k,v in monthly_reports.items():
                report1[k] = v['usage'].sum()
            
            df_report1 = pd.DataFrame(list(report1.items()), columns= ['Month', 'Total Usage'])
            print(df_report1.to_string(index=False))

        elif report == '2':
            report2 = {}
            customerID = 0
            while int(customerID) not in range(1,1000):
                customerID = input('enter customerID (1-999): ')
            
            for k,v in monthly_reports.items():
                unsorted_df = v[v['customerID'] == int(customerID)].groupby('serviceID').size().reset_index(name='counts')
                report2[k] = list(unsorted_df.sort_values('counts', ascending=False).head(3)['serviceID'])

            df_report2 = pd.DataFrame(list(report2.items()), columns= ['month', 'top_three_used_sevices'])
            print(df_report2.to_string(index=False))

            

        elif report == '3':
            report3 = {}
            for k,v in monthly_reports.items():
                country_usage = v.groupby('country').agg({'usage': 'sum'})
                report3[k] = country_usage.apply(lambda x : 100 * x / float(x.sum())).round(decimals=2)
            
            report_3 = {}
            for k,v in report3.items(): 
                report_3[k] = [str(x) + '%' for x in list(v['usage'])]
            
            df_report3 = pd.DataFrame.from_dict(report_3, columns= ['CN', 'UK', 'US', 'FR', 'DE', 'ES', 'CA', 'IN', 'JP', 'KR', 'SG'], orient='index')
            print(df_report3)