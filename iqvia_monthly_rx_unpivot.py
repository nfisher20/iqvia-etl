
import pandas as pd
import dateutil.relativedelta
import datetime
import sys
import os


def getMonthList(date,monthsPrior):

    startdate = datetime.datetime.strptime(date, "%Y-%m-%d")
    enddate = startdate - dateutil.relativedelta.relativedelta(months=monthsPrior)

    #use the pandas daterange function to generate a list of months
    x = pd.date_range(enddate,startdate, freq='MS').strftime("%Y-%m-%d").tolist()

    #revers lists of months
    x.reverse()

    #get rid of - characters in list as date_range generates a list separating them
    months = [i.replace('-','/')for i in x]
    return months


def UnpivotAndSaveFile(monthRange,filepath,source,savedir,monthname):
    #source files did not have headers or equal number of columns, headers are explicit so that files can be unioned in ADF
    if source == 'Provider2':
        headers = ['ClientNumber', 'ReportNumber', 'IQVIAPrescriberNumber', 'IQVIAPlanIDNumber', 'SourceSpecialtyCode',
               'NDC', 'SalesCategory', 'RxType', 'ProductGroupNumber', 'Column10', 'MENumber', 'PrescriberLastName',
               'PrescriberFirstName', 'PrescriberMI', 'PrescriberAddress', 'PrescriberCity', 'PrescriberState',
               'PrescriberZip', 'NPINumber', 'PayerPlan', 'DataDate', 'BucketCount']
    elif source == 'Provider1':
        headers = ['ClientNumber', 'ReportNumber', 'IQVIAPrescriberNumber', 'IQVIAPlanIDNumber', 'SourceSpecialtyCode',
               'NDC', 'SalesCategory', 'RxType', 'ProductGroupNumber', 'Column10', 'MENumber', 'PrescriberLastName',
               'PrescriberFirstName', 'PrescriberMI', 'PrescriberAddress', 'PrescriberCity', 'PrescriberState',
               'PrescriberZip', 'NPINumber', 'PayerPlan', 'NDCDescription', 'DataDate', 'BucketCount']
    elif source is None:
        print('Must provide data source')
        sys.exit(0)

    #quantitive columns are pivoted and need to have dates assigned dynamically
    #s uses list comprehension to create a list of metrics with attached months, separated by a comma
    buckets = ['NewRx','TotalRx','NewQuant','TotalQuant']
    s = [i+','+j for i in buckets for j in monthRange]

    headers = headers+s

    df = pd.read_csv(filepath,dtype = str,names = headers, parse_dates=[20])

    columns = list(df)

    if source == 'Provider1':
        static = columns[:22]
        scripts = columns[22:]
    elif source == 'Provider2':
        static = columns[:23]
        scripts = columns[23:]

    #unpivoting of metric data
    df2 = pd.melt(df,
                      id_vars=static,
                      value_vars=scripts,
                      var_name='Desc',
                      value_name='Value')


    #separation of metrics and date column
    df2 = pd.concat([df2,df2['Desc'].astype(str).str.split(',',expand=True)],axis=1)

    df2['Quantity'] = df2['Value'].astype(float)

    #drop original unseparated columns
    df2.drop(columns = ['Desc','Value'],inplace=True)

    #rename split columns
    df2.rename(columns = {0:'Prescriptions',1:'PrescriptionMonth'},inplace=True)

    if source == 'Provider1':
        path = os.path.join(savedir,f'Provider1{monthname}.txt')
    elif source == 'Provider2':
        path = os.path.join(savedir,f'Provider2{monthname}.txt')

    #save csv's locally
    df2.to_csv(path, index=False)

    return path



