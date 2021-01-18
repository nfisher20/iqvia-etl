import os
import shutil
import sys
import re

from azure.core.exceptions import ResourceExistsError

import azure_datafactory
import azure_blob
import iqvia_file_manipulation
import iqvia_monthly_rx_unpivot

#instantiate blob and adf clients, create container client objects
aytuadf = azure_datafactory.InitiateDatafactoryClient()
blobserviceclient = azure_blob.initiateBlobServiceClient()

#assign blob containers to variables
iqviaclient = azure_blob.returnContainerClient(blobserviceclient,'iqvia')
iqviarxweeklyclient = azure_blob.returnContainerClient(blobserviceclient,'iqviarxweekly')
iqviamonthlyclient = azure_blob.returnContainerClient(blobserviceclient,'iqviamonthly')


currentdirectory = os.getcwd()

#run methods from file_manipulation
iqvia_file_manipulation.UnzipAllFolders(currentdirectory,True)

iqvia_file_manipulation.RenameRxWeeklyByNDC(currentdirectory)

filestoupload = iqvia_file_manipulation.GenerateFilesToUpload(currentdirectory)

#instantiate monthly load
monthlyload = False


#v[0] is the full path and file name
#v[1] is the file name
#v[2] is the folder path
for k, vs in filestoupload.items():
    for v in vs:
        #monthly load process
        if 'ZTWRX07' in v[0] or 'ZTWRY03' in v[0]:

            #variable used later to trigger monthly ingest pipeline
            monthlyload = True

            #create the month list used for the unpivoting process
            datamonth = re.search(r"M\d{4}",v[1]).group(0)
            datamonthstr = '20'+datamonth[3:5]+'-'+datamonth[1:3]+'-01'
            monthlist = iqvia_monthly_rx_unpivot.getMonthList(datamonthstr,24)

            #ZTWRX07 is Provider1
            if 'ZTWRX07' in v[0]:
                uploadpath = iqvia_monthly_rx_unpivot.UnpivotAndSaveFile(monthlist,v[0],'Provider1',v[2],datamonth)

                try:
                    with open(uploadpath, "rb") as data:
                        iqviamonthlyclient.upload_blob(name= 'Provider1/Provider1'+datamonth+'.txt', data=data)
                except ResourceExistsError:
                    print(f'Upload failed for {k}, blob already exists')

            #ZTWRY03 is Provider2
            elif 'ZTWRY03' in v[0]:
                uploadpath = iqvia_monthly_rx_unpivot.UnpivotAndSaveFile(monthlist, v[0], 'Provider2', v[2], datamonth)

                try:
                    with open(uploadpath, "rb") as data:
                        iqviamonthlyclient.upload_blob(name='Provider2/Provider2' + datamonth + '.txt', data=data)
                except ResourceExistsError:
                    print(f'Upload failed for {k}, blob already exists')

            #Move loaded csv's to Loaded To Blob
            shutil.move(v[2], os.path.join(os.getcwd(), 'Loaded To Blob', k))

        #rx weekly load process, loads to blob, runs pipeline, moves csv to Loaded To Blob/RX Weekly Loaded and deletes root directory
        elif 'RX Weekly - By NDC ' in v[1]:

            try:
                with open(v[0], "rb") as data:
                    iqviarxweeklyclient.upload_blob(name=v[1], data=data)

                #if the blob upload fails, the except will be triggered and pipeline not run
                azure_datafactory.RunPipeline(aytuadf,"IQVIA Rx Weekly Split",delaystart=50,delayrunstatuscheck=300)

            except ResourceExistsError:
                print(f'Upload failed for {k}, blob already exists')

            shutil.move(v[0], os.path.join(os.getcwd(), 'Loaded To Blob', 'RX Weekly Loaded'))
            os.rmdir(v[2])

        #moves other RX Weekly directories that are not the correct weekly files, this could happen in file_manipulation somewhere
        elif "RX Weekly" in v[1]:
            shutil.move(v[0], os.path.join(os.getcwd(), 'Loaded To Blob'))

        #uploads all other files to the blob and moves them to Loaded To Blob
        elif "RX Weekly" not in v[0]:
            try:
                with open(v[0], "rb") as data:
                    iqviaclient.upload_blob(name=k + '/' + v[1], data=data)
            except ResourceExistsError:
                print(f'Upload failed for {k}, blob already exists')

            shutil.move(v[2], os.path.join(os.getcwd(), 'Loaded To Blob', k))

#runs the monthly ingest pipeline if new monthly data was received, exits otherwise
if monthlyload:
    azure_datafactory.RunPipeline(aytuadf, "IQVIAsplit", delaystart=50, delayrunstatuscheck=600)
else:
    print('IQVIA extraction and loading process has finished, no monthly data present')
    sys.exit()