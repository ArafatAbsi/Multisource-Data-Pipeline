import paramiko,os,sys,time,re,glob,csv,pyodbc,calendar,psutil,gzip,shutil,ftplib,warnings
from pandas import  read_excel,ExcelFile  ; from datetime import datetime,timedelta
import pandas as pd  ; from time import strftime  ; from shutil import move
from zipfile import ZipFile  ;  from os import chdir ; import numpy as np
from socket import gethostname,gethostbyaddr,gethostbyname
from sqlalchemy import create_engine ;
#from fsplit.filesplit import FileSplit
from multiprocessing import cpu_count , Pool
from smb.SMBConnection import SMBConnection
warnings.simplefilter('ignore', category=UserWarning)
#####################################

def getFiles(ne,Device_Data):
    '''
    This Function fetches input Files according to Input parameters 
    
    Input: Protocol Name
    Output: Files (collect files)
    '''
    
    Local_IP, Remote_IP,Remote_HostName, Protocol, Username, Password, Shared_Folder1, Shared_Folder2, Dir , FileType,L_dir,Last_DownloadedFile = split_para(Device_Data)
    Fnames=[]

    if Protocol=='SFTP':
        Fnames = SFTP_Session(ne,Device_Data)
    elif Protocol=='smb':
        Fnames=get_SMB_Files(ne,Device_Data)
    elif Protocol=='locally':
        Fnames=get_Local_Files(ne,FileType,Dir)
    else:
        print("This protocol is not supported in our system")
    
    return Fnames
    
    
######################################
def Last_Downloaded_Files(Last_DownloadedFile):
    Last_Processed_FileName=''
    os.chdir("C:/Multisource_Data_Pipeline/Project_Package/Last_Downloaded_Files/")
    for filename in sorted(glob.glob("*.txt")):
        if Last_DownloadedFile in filename:
            Last_Processed_FileName= filename
    return Last_Processed_FileName

######################################
def SFTP_Session(ne,NeData):
    try:
        files_In_Dir=[]
        ready_files =[]
        
        Local_IP, Remote_IP,Remote_HostName, Protocol, Username, Password, Shared_Folder1, Shared_Folder2, Dir , FileType,L_dir,Last_DownloadedFile = split_para(NeData)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(Remote_IP, username=Username, password=Password)
        Yesterday_Date=datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
        
        with ssh.open_sftp() as sftp:
            R_dir=Dir.split(';')
            
            for index in range(len(R_dir)):
                os.chdir(L_dir) #specify the path to where you want to download files 
                RemoteDirectory=R_dir[index]

                sftp.chdir(RemoteDirectory)
                
                for filename in sftp.listdir():
                    filesize = sftp.stat(filename).st_size #get file size and download not_empty files
                    if eval(FileType) : #to download files with specific extenstion as written in excel sheet
                        NewFileName=filename
                        if  ne.startswith("Source3_PerformanceFiles_SBC"):
                            NewFileName=RemoteDirectory.split('/')[-3]+"_"+RemoteDirectory.split('/')[-2]+"_"+filename.replace('+03_','')
                        if Last_DownloadedFile =='Not_Used' :
                            #in the remote directory we can move files from folder to folder in order
                            #to avoid the old method (below) of searching the last downloaded file which is slow when thousands on files exist in the directory
                            if filesize>0:
                                sftp.get(filename,NewFileName)
                                if 'Downloaded_Files' in L_dir:
                                    ready_files.append(NewFileName)
                                if ne.startswith("Source3_PerformanceFiles_SBC"):
                                    #rename(oldpath, newpath) the rename method here can move files from path to path
                                    sftp.rename(RemoteDirectory+filename,RemoteDirectory+'Older/'+filename)
                                
            if Last_DownloadedFile !='Not_Used' :
                #get name of last file processed file, to download files after it
                Last_Processed_FileName= Last_Downloaded_Files(Last_DownloadedFile)
                Last_Processed_FileNo= Last_Processed_FileName.split('_')[0]
                files_In_Dir.sort(reverse=False)
                for filename in files_In_Dir:
                    file_being_processed=''
                            
                    if ne.startswith("IGW6_Processing_cdr"):
                        file_being_processed=filename.split('-')[0].replace('cdr','')
                    else:
                        file_being_processed=filename.split('.')[0].split('-')[-1]
                        
                    filesize = sftp.stat(filename).st_size
                    if filesize>0:
                        sftp.get(filename,filename)
                        if 'Downloaded_Files' in L_dir:
                            ready_files.append(filename)
                         
                    FilesDir="C:/Multisource_Data_Pipeline/Project_Package/Last_Downloaded_Files/"
                    OldFile=Last_Processed_FileNo+Last_DownloadedFile
                    NewFile=file_being_processed+Last_DownloadedFile
                    shutil.move(FilesDir+OldFile,FilesDir+NewFile)
                    Last_Processed_FileNo=file_being_processed
                    print(Last_Processed_FileName,Last_Processed_FileNo,file_being_processed)
                    
                       
        sftp.close()  
        ssh.close()
        return ready_files
    except Exception as e:
        Errors_Archiving(str(e))
        pass   

######################################
def get_SMB_Files(ne,NeData):
    '''
    This Function downloads Files if the protocol used is smb
    
    Input: network element parameters from the excel sheet (Input_Data) in order to pass smb related arguments to start collecting files using smb
    Output: Files (downloaded or created as output of executed commands)
    '''
    lFilenames = []
    newFileNames = []
    PerfFntPath = 'C:/Multisource_Data_Pipeline/Downloaded_Files/'
    TodaysDate= time.strftime("%d-%m-%Y")
    os.chdir(PerfFntPath)
    
    #parameters from the excel sheet 
    Local_IP, Remote_IP,Remote_HostName, Protocol, Username, Password, Shared_Folder1, Shared_Folder2, Dir , FileType,L_dir,Last_DownloadedFile = split_para(NeData)

    conn = smbConn(Username, Password, gethostname(), Remote_HostName, Remote_IP)
    
    sharedFile = conn.listPath(Shared_Folder1, Shared_Folder2+'/' )

    for file in sharedFile:
        
        if ne.startswith("Source1_Performance"):
            lFilenames.append(file.filename)
        elif ne.startswith("Help_Desk") and TodaysDate in file.filename:
            if '.xlsx' in file.filename:
                lFilenames.append(file.filename)
                

    #rename files after download as per our needs
    for filename in lFilenames:
        try:
            openFile = open(filename, 'wb')
            file_attributes, filesize = conn.retrieveFile(Shared_Folder1, Shared_Folder2+'/'+ filename, openFile)
            openFile.close()
            if  ne.startswith("Source1_Performance"):
                NewFileName=''
                if "hourly_in" in filename:
                    NewFileName=Remote_HostName.split('-')[0]+'_In_'+strftime("%Y_%m_%d")+'.csv'
                elif "hourly_out" in filename:
                    NewFileName=Remote_HostName.split('-')[0]+'_Out_'+strftime("%Y_%m_%d")+'.csv'
                elif "INLET" in filename:
                    NewFileName=Remote_HostName.split('-')[0]+'_InLet_'+strftime("%Y_%m_%d")+'.csv'
                elif "OUTLET" in filename:
                    NewFileName=Remote_HostName.split('-')[0]+'_OutLet_'+strftime("%Y_%m_%d")+'.csv'
                newFileNames.append( NewFileName)
                move(filename, NewFileName)
                
            #nothing to do with file name here, so return as it is
            elif ne.startswith("Help_Desk"):  
                newFileNames.append(filename)
                                            
        except Exception as e:
            Errors_Archiving(str(e))
            Errors_Archiving("We faced a problem to download ("+ filename+ ") from the server")
            continue
    print("The Files under ("+ Remote_HostName.split('-')[0]+ "'s) has been downloaded successfully.")

    
    conn.close()
    return newFileNames        
     
######################################
def smbConn(uname, pword, lHost, dHost, dAddr):
    conn = SMBConnection(uname, pword, lHost, dHost, use_ntlm_v2=True)
    try:
        conn.connect(dAddr)
        print("The (", dAddr, ") server has been connected successfully using SMB protocol")
        return conn
    except Exception as e:
        Errors_Archiving("Couldn't log to the server (" + dAddr + "). Please recheck your input data...")
        Errors_Archiving(str(e))
        
        #exit(1)
######################################

def get_Local_Files(ne,FileType,Local_Directory):
    '''
    This Function collects files already exists in the same server, that bieng pushed by remote servers to our local server
    Input: NE parameters
    Output: Files collected from mutliple direcotries to the downloaded folder (central point where all files are being processed )
    This Func stopped reprocessing the corrupted files, and isolated them besides making the monitoring easier 
    '''
            
    Fnames = []
    

    if os.path.isdir(Local_Directory):
        os.chdir(Local_Directory)
        #big file that having big size are being splitted into 50MB files
        Check_Files_With_Large_Size(Local_Directory,FileType)
        if  ne.startswith("Source1_Performance") or ne.startswith("Source2_CDR") :
            for filename in sorted(glob.glob("*"+FileType)):
                Fnames.append(filename)
                shutil.move(filename ,'C:/Multisource_Data_Pipeline/Downloaded_Files/'+ filename )
        
    return Fnames

######################################
def Observe_CDR_Files_Directory():
    '''
    waiting for CDR Files to arrive
    '''
    
    DIR = 'C:\\Multisource_Data_Pipeline\\CDR_Processing\\CDR_Files\\'
    while True:
        if len(glob.glob1(DIR,"*.dat"))>1:
            break
        time.sleep(1)
        if str(datetime.strftime(datetime.now() , '%M:')) >'10:':
            break

######################################
def Check_Files_With_Large_Size(Local_Directory,FileType):
    '''
    this fun splits files with size > 50 M
    '''
    
    os.chdir(Local_Directory)
    for filename in sorted(glob.glob("*"+FileType)):
        if os.stat(filename).st_size/(1000*1034)>50:
            fs = FileSplit(file=filename, splitsize=50000000, output_dir=Local_Directory)
            fs.split()
            if filename.endswith('.dat'):
                os.remove(filename)

######################################
def Errors_Archiving(log_error):
    '''
    this func archives the executing Errors.
    '''
    print(log_error)
    Output_Writer = open('C:/Multisource_Data_Pipeline/ErrorFile.txt', 'a')
    Output_Writer.write(strftime('%d_%m_%Y %H:%M:%S')+'   '+log_error+'\n')
    Output_Writer.close()
######################################
def Logs_Archiving(log_line):
    '''
    this func archives the executing LOGs.
    '''
    print(log_line)
    Output_Writer = open('C:/Multisource_Data_Pipeline/logFile.txt', 'a')
    Output_Writer.write(strftime('%d_%m_%Y %H:%M:%S')+' '+log_line+'\n')
    Output_Writer.close()
    
def split_para(data):    
    return data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8],data[9],data[10],data[11]



