import paramiko,os,sys,time,re,glob,csv,pyodbc,calendar,psutil,gzip,shutil,ftplib,warnings
from pandas import  read_excel,ExcelFile  ; from datetime import datetime,timedelta
import pandas as pd  ; from time import strftime  ; from shutil import move
from zipfile import ZipFile  ;  from os import chdir ; import numpy as np
from socket import gethostname,gethostbyaddr,gethostbyname
from sqlalchemy import create_engine ;
#from fsplit.filesplit import FileSplit
from multiprocessing import cpu_count , Pool
from smb.SMBConnection import SMBConnection
from Project_Package import *
warnings.simplefilter('ignore', category=UserWarning)
#####################################
def NE_Input_Para(neFile):
    '''
    this func reads the excel file and will iterate through all Network Elements
    and get parameters needed (IPs, Usernames, Passwords etc.) to access the network element in order to fetch the needed data 
    '''
    NeData = {}
    for i in range(len(neFile)): 
        NeData[neFile['Report_Name'][i]] = neFile['Local_IP'][i],neFile['Remote_IP'][i],neFile['Remote_HostName'][i], neFile['Protocol'][i], neFile['Username'][i], neFile['Password'][i], \
                                           neFile['Shared_Folder1'][i], neFile['Shared_Folder2'][i], neFile['Path'][i], neFile['FileType'][i], \
                                           neFile['L_dir'][i] ,neFile['Last_DownloadedFile'][i]
    return NeData
##########################################
def split_para(data):    
    return data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8],data[9],data[10],data[11]
##########################################  

if __name__ == '__main__':
    os.chdir('C:/Multisource_Data_Pipeline/Downloaded_Files/')
    NeData= NE_Input_Para(read_excel(confFile, sheet_name='Collect_Data_Sheet', skiprows=[0]))
    for ne in NeData.keys():
        Local_IP,Remote_IP,Remote_HostName,Protocol,Username,Password,Shared_Folder1,Shared_Folder2,Dir,FileType,L_dir,Last_DownloadedFile = split_para(NeData[ne])
        Processed_Files=[]
        if gethostbyname(gethostname())==Local_IP:
            Fnames = getFiles(ne,NeData[ne])
            if len(Fnames)>0 :             
                for filename in Fnames:
                    FileInfo= Get_File_Info(ne,filename)
                    archive_flag,read_para,store_para,archive_para=FileInfo.split('|')
                    try:
                        if archive_flag != 'yes':  # -----> files which processed and stored in databases
                            Processed_Files.append(filename)
                            records = read_file(ne,filename,read_para)
                            records = format_fields(ne,filename,records)
                            records = process_pandas_data(map_values, records, cpu_count(), filename)
                            records = add_fields(ne,filename,records)
                            store_data(ne,filename,records,store_para)
                            Archive_Files(ne,filename,archive_para)
                        else: # -----> files need to be archived only as raw data without storing in databases
                            Archive_Files(ne,filename,archive_para)
                        
                    except Exception as e:
                        Errors_Archiving(str(e))
                        continue
                if ne.startswith("Source2_CDR") and len(Processed_Files)>0 :
                    execute_SQL_Scripts(ne)
  
    
    Logs_Archiving("Task Execution Finished")
    
