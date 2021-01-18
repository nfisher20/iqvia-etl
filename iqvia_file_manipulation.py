import datetime
import os
import zipfile

def UnzipAllFolders(currentfolder,removepath):

    for r,d,f in os.walk(currentfolder):
        for file in f:
            #check if ZIP exists in the file name
            if '.ZIP' in file.upper():
                path = os.path.join(r, file)
                # if RxWeekly is in the file name, get the date from the file inside and append it to the file name
                if 'RxWeekly(NPA)' in file:
                    with zipfile.ZipFile(path, 'r') as zipObject:
                        for i in zipObject.infolist():
                            filedate = datetime.datetime(*i.date_time[0:6])
                            filedateabb = filedate.strftime("%b %d %Y %H %M %S")
                            savepath = path[:path.upper().find('.ZIP')] + ' ' + filedateabb
                            # break
                else:
                    savepath = path[:path.upper().find('.ZIP')]
                # extract all zipped files, save as savepath
                with zipfile.ZipFile(path) as zip_ref:
                    zip_ref.extractall(savepath)
                    #delete zip files
                if removepath == True:
                    os.remove(path)


def RenameRxWeeklyByNDC(currentfolder):
    #walk the current directory and rename the Rx Weekly - By NDC by Class files
    for r, d, f in os.walk(currentfolder):
        for dir in d:
            if 'RxWeekly(NPA)' in dir:
                if (dir != 'Loaded To Blob') and (dir != '.idea') and (dir != '__pycache__') and (
                        "RAD" not in dir) and (
                        '.ipynb_checkpoints' not in dir) and 'Loaded To Blob' not in r:
                    for sr, sd, sf in os.walk(os.path.join(r, dir)):
                        for f in sf:
                            #rename rx weekly file to include the date of the file
                            if 'Rx Weekly - By NDC by Class (Monday Rep)' in f:
                                os.rename(os.path.join(sr, sf[0]),
                                          os.path.join(sr, 'RX Weekly - By NDC' + dir[dir.find('_') + 2:] + '.csv'))

def GenerateFilesToUpload(currentfolder):
    #walks the directory provided and adds files not in Loaded to Blob to a dictionary to be looped through
    filestoupload = {}
    for r, d, f in os.walk(currentfolder):
        for dir in d:
            if (dir != 'Loaded To Blob') and (dir != 'inspectionProfiles') and (dir != '__pycache__') and (
                    dir != '.idea') and ("RAD" not in dir) and (
                    '.ipynb_checkpoints' not in dir) and 'Loaded To Blob' not in r:
                filestoupload[dir] = []
                for sr, sd, sf in os.walk(os.path.join(r, dir)):
                    for file in sf:
                        filestoupload[dir].append([os.path.join(sr, file), file, sr])
    return filestoupload