


import numpy as np
import pandas as pd
import os,shutil
import sys
sys.path.append('/Users/ayshahchan/Desktop/PhD/eurocropsV2/EuroCropsV2/code/utils/')
from tools import *
import subprocess
from glob import glob

from joblib import Parallel, delayed





postgis_cfg = global_config.postgis


ScheMa = postgis_cfg['pg_gsa_schema']

FileList = '/Users/ayshahchan/Desktop/PhD/eurocropsV2/EuroCropsV2/data/processing/se_original_dataset.csv'
df = pd.read_csv(FileList)

FileList = '/Users/ayshahchan/Desktop/PhD/eurocropsV2/EuroCropsV2/data/processing/se_columns_listing.csv'
df_columns = pd.read_csv(FileList)

DirInEos = global_config.paths['originaldataset_dir'] # where are located the orifginal data set
DirInScr = global_config.paths['fastio_dir']

dfout=[]


def ProcRow(row):
    PrintLog(row['name'])
    CheckTable= GetSQL("SELECT EXISTS (SELECT FROM information_schema.tables "+\
                "WHERE  table_name     = '"+row['name']+"' "+\
                "AND    table_schema   = '"+ScheMa+"'); ")
    # copy source file from one location to another if not already there (to avoid multiple access on the same file from multiple cores)
    # filename1 = DirInEos+row.Nuts+'/'+row.path
    filename1 = DirInEos+row.path
    # filename2 = DirInScr   +   row['path'].split('/')[-1]
    filename2 = DirInScr   +   row.path
    
    if not os.path.exists(filename1):
        PrintLog(f"ERROR: Source file does not exist: {filename1}")
        return [row['name'], np.nan, np.nan, np.nan]
    
    if len(glob(filename2))==0:

        if filename1[-4:]=='.shp':
            for f in glob(filename1[:-4]+'.*'):
                
                shutil.copy(f,DirInScr+f.split('/')[-1])
        else:
            shutil.copy(filename1,filename2)

        # Verify the destination file was created/exists
    if not os.path.exists(filename2):
        PrintLog(f"ERROR: Destination file was not created: {filename2}")
        return [row['name'], np.nan, np.nan, np.nan]        
    
   


    Command = f'ogrinfo "{filename2}" -dialect sqlite -sql "SELECT COUNT(*) c FROM \\"{row.layer}\\" "'
    # out = os.popen(Command).read()
    out = os.popen(Command + ' 2>&1').read()
    print(out, flush=True)

    
    N_original = int(out.split(" ")[-1].replace('\n',''))

    ListCol = ','.join(df_columns[(df_columns.nuts==row.Nuts)&(df_columns.year==row.year)&(df_columns.ToKeep==1)].column_name.tolist())

    if CheckTable.iloc[0,0]==False:
        try:
            # Check if layer has CRS
            info_cmd = f'ogrinfo -al -so "{filename2}"'
            info_result = subprocess.run(info_cmd, shell=True, capture_output=True, text=True)

            # Look for SRS in output
            has_crs = 'GEOGCS' in info_result.stdout or 'PROJCS' in info_result.stdout or 'EPSG' in info_result.stdout

            # Build command conditionally
            s_srs_flag = '-s_srs EPSG:3035' if not has_crs else ''

            Command = 'ogr2ogr -f "PostgreSQL" '+s_srs_flag+' -t_srs EPSG:3035 -makevalid -skipfailures '+\
                      'PG:"host='+postgis_cfg['pg_host']+' port='+postgis_cfg['pg_port']+' user='+postgis_cfg['pg_user']+\
                      ' dbname='+postgis_cfg['pg_dbname']+' password='+postgis_cfg['pg_password']+'" '+filename2+\
                      ' '+row.layer+' -nln '+row['name']+\
                      ' -nlt MULTIPOLYGON -lco SCHEMA="'+ScheMa+\
                        '" -lco OVERWRITE=YES -lco precision=NO -dim XY -select "'+ListCol+'"'


            os.system(Command)

            N_final = GetSQL('select count(*) c from '+ScheMa+'.'+row['name']).iloc[0,0]

            if N_final==N_original:
                PrintLog(row['name'].ljust(10)+ ': IMPORTED FULL',taille=40)
            else:
                PrintLog(row['name'].ljust(10)+ ': IMPORTED but FAILED on ' +str(np.round(100-N_final/N_original*100,decimals=4))+'% ('+str(N_original-N_final)+'/'+str(N_original)+')',taille=40)
        except:
            PrintLog(row['name'].ljust(10)+ ': FAILED',taille=40)
            N_final = np.nan
    else:
        N_final = GetSQL('select count(*) c from '+ScheMa+'.'+row['name']).iloc[0,0]
        if N_final==N_original:
            PrintLog(row['name'].ljust(10)+ ': ALREADY PROC. FULL',taille=40)
        else:
            PrintLog(row['name'].ljust(10)+ ': ALREADY PROC. '+str(np.round(100-N_final/N_original*100,decimals=4))+'% ('+str(N_final-N_original)+'/'+str(N_original)+')',taille=40)
            print('DROP TABLE '+ScheMa+'.'+row['name']+';')
    return [row['name'],N_original,N_final,N_final/N_original]




dfout= Parallel(8, verbose=0,timeout=99999)(delayed(ProcRow)(row) for index, row in df.iterrows())

dfout = pd.DataFrame(dfout,columns=['table','N_original','N_final','Ratio'])

dfout.to_csv(DirInEos+'Import_report_'+dt.datetime.now().strftime('%Y%m%d%H%M')+'.csv')
