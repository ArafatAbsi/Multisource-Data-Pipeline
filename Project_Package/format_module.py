import paramiko,os,sys,time,re,glob,csv,pyodbc,calendar,psutil,gzip,shutil,ftplib,warnings
from pandas import  read_excel,ExcelFile  ; from datetime import datetime,timedelta
import pandas as pd  ; from time import strftime  ; from shutil import move
from zipfile import ZipFile  ;  from os import chdir ; import numpy as np
from socket import gethostname,gethostbyaddr,gethostbyname
from sqlalchemy import create_engine ;
#from fsplit.filesplit import FileSplit
from multiprocessing import cpu_count , Pool
from smb.SMBConnection import SMBConnection
#import re
warnings.simplefilter('ignore', category=UserWarning)
#####################################

cdr_file = ExcelFile('C:/Multisource_Data_Pipeline/CDR_Processing/CDR_Data.xls')
confFile = 'C:/Multisource_Data_Pipeline/Project_Package/Input_Data.xlsx'

#Prepare Source2_CDR data by mapping its values in dectionaries   
cdr_columns_file = read_excel(cdr_file, 'cdr_columns', dtype=str)
d_cdr_columns = {}
for i in range(len(cdr_columns_file)):
    d_cdr_columns[cdr_columns_file['ColumnName'][i]] = cdr_columns_file['ColumnType'][i]

country_code_file = read_excel(cdr_file, 'country_code', dtype=str)
d_country_code = {}
for i in range(len(country_code_file)):
    d_country_code[country_code_file['CountryCode'][i]] = country_code_file['CountryName'][i]

bill_type_file = read_excel(cdr_file, 'bill_type')
d_bill_type = {}
for i in range(len(bill_type_file)):
    d_bill_type[bill_type_file['digit'][i]] = bill_type_file['description'][i]

termination_code_file = read_excel(cdr_file, 'termination_code')
d_termination_code = {}
for i in range(len(termination_code_file)):
    d_termination_code[termination_code_file['digit'][i]] = termination_code_file['description'][i]


d_map_value = [{'bill_type': d_bill_type}, {'termination_code': d_termination_code}]


##########################################
#read the Format_Data_Sheet in excel file (Input_Data)
File_Info_Dic = {}
File_Info_Data=read_excel(confFile, sheet_name='Format_Data_Sheet', skiprows=[0])
for i in range(len(File_Info_Data)):
    File_Info_Dic[File_Info_Data['ne_and_filename'][i]] = File_Info_Data['archive_only_flag'][i] +'|'+ File_Info_Data['read_para(sep,skiprows,usecols,header,dtype)'][i]+'|'+ File_Info_Data['store_para(IP,user,password,DB,Table,Schema)'][i]+'|'+ File_Info_Data['archive_para(directory)'][i]


##########################################
def Get_File_Info(ne,filename):

    FileInfo=""
    
    try:
        for FilePara in File_Info_Dic.keys():
            
            #ifFilePara.split(';')[1]== filename[0:len(FilePara.split(';')[1])]:
            FilePara.split(';')[1]
            if FilePara.split(';')[1] in filename and (FilePara.split(';')[0] in ne or 'To_be_Reprocessed' in ne):
                FileInfo=File_Info_Dic[FilePara]
                return FileInfo
    except:
        FileInfo=""
        return FileInfo
    
##########################################
def Prepare_Source1_Perf_Files(ne,filename,read_para):
    lUselessHourRow = [i for i in range(1, 100500, 2)]; lUselessHourRow.append(0)
    lUselessHourTG = [i for i in range(0, 100500, 2)]
    lUselessIORow = [i+2 for i in range(1, 301000) if i % 6 != 0]; lUselessIORow.insert(0, 1); lUselessIORow.insert(0, 0)
    lUselessIOTGin = [i+3 for i in range(1, 301000) if i % 6 != 0]; lUselessIOTGin.insert(0, 2); lUselessIOTGin.insert(0, 0)
    lUselessIOTGout = [i+4 for i in range(301000) if i % 6 != 0]; lUselessIOTGout.insert(0, 3); lUselessIOTGout.insert(0, 2)
    lUselessIOTGout.insert(0, 0)
    if '_In_' in filename or '_Out_' in filename:
        DF = pd.DataFrame(pd.read_csv( filename, skiprows = lUselessHourRow, sep='\t'))
        TG = pd.DataFrame(pd.read_csv( filename, header=None, skiprows=lUselessHourTG, skip_blank_lines=True, sep=' ')[6])     
    elif '_InLet_' in filename:
        DF = pd.DataFrame(pd.read_csv( filename, skiprows=lUselessIORow, sep='\t'))
        TG = pd.DataFrame(pd.read_csv( filename, header=None, skiprows=lUselessIOTGin, skip_blank_lines=True, sep=' ')[4])
    elif '_OutLet_' in filename:
        DF = pd.DataFrame(pd.read_csv( filename, skiprows=lUselessIORow, sep='\t'))
        TG = pd.DataFrame(pd.read_csv( filename, header=None, skiprows=lUselessIOTGout, skip_blank_lines=True, sep=' ')[3])
    TG = TG.iloc[:,0] #to convert TG to series in order to insert it inside the DF
    DF = DF.iloc[:, :-1]
    DF.insert(1, "Trunk Name", TG)
    return DF

##########################################   
def read_file(ne,filename,read_para):
    '''
    Input: file to be read
    Output: DF arry of the input file
    '''
    processing_directory='C:/Multisource_Data_Pipeline/Downloaded_Files/'
    os.chdir(processing_directory)
    try:
        if read_para != 'None':
            Separator,SKIP_ROWS,COLOMNS,HEAD,DataTypes=read_para.split('!')
            #skiprows=[2] to skip line @ index 1 only
            #skiprows=2 number of lines to skip
            skip = [int(SKIP_ROWS)] if (ne.startswith("Source3__IGW6")) else int(SKIP_ROWS)
            DF = pd.DataFrame(pd.read_csv(filename, sep=Separator, skiprows=skip, usecols=eval(COLOMNS),header =eval(HEAD),dtype=eval(DataTypes),low_memory=False))
        else:
            if ne.startswith("Source1_Performance"):
                DF = Prepare_Source1_Perf_Files(ne,filename,read_para)
    
        Logs_Archiving("File ("+ filename+ ") has been read successfully ")
        
        return DF
    except Exception as e:
        Errors_Archiving('Phase read_file  '+ne +'   '+filename+'   '+str(e))
        pass


##########################################
def format_fields(ne,FileName ,DF, ras_columns=d_cdr_columns):
    '''
    :param cdr_records:
    :return:
    '''
    try:
        if ".dat" in  FileName:
            
            DF.columns = ras_columns.keys()
            
            DF['conversation_time'] = pd.to_numeric(DF['conversation_time'])
            DF['end_time'] = pd.to_datetime(DF['end_time'], format='%Y-%m-%d %H:%M:%S')
            end_conv = DF.loc[DF.start_time == '                   ', ['end_time', 'conversation_time']]
            DF.loc[DF.start_time == '                   ', 'start_time'] = [str(data[0] - pd.Timedelta(seconds=data[1])) for data in end_conv.values]
            DF.loc[DF.TMG_Circuits_seizure_time == '                   ', 'TMG_Circuits_seizure_time'] = DF.end_time
            DF.loc[DF.TMG_Circuits_release_time == '                   ', 'TMG_Circuits_release_time'] = DF.end_time
            DF.loc[DF.Start_Date_and_Time_of_Call_Setup == '                   ', 'Start_Date_and_Time_of_Call_Setup'] = DF.end_time

            DF['caller_number'] = DF.apply(lambda row: str(row.caller_number).strip(), axis=1)
            DF["caller_number"] = DF["caller_number"].apply(lambda x: str(x).replace(str(x)[:2],'') if str(x).startswith('00') else x )
            DF['called_number'] = DF.apply(lambda row: str(row.called_number).strip(), axis=1)
            DF['connected_number'] = DF.apply(lambda row: str(row.connected_number).strip(), axis=1)
            DF['dialed_number'] = DF.apply(lambda row: str(row.dialed_number).strip(), axis=1)
            DF['Incoming_Route_ID'] = DF.apply(lambda row: row.Incoming_Route_ID.strip(), axis=1)
            DF['Outgoing_Route_ID'] = DF.apply(lambda row: row.Outgoing_Route_ID.strip(), axis=1)
            DF['local_switch_name'] = DF.apply(lambda row: row.local_switch_name.strip(), axis=1)
            DF[['start_time','end_time','TMG_Circuits_seizure_time','TMG_Circuits_release_time','Start_Date_and_Time_of_Call_Setup']] = DF[['start_time','end_time','TMG_Circuits_seizure_time','TMG_Circuits_release_time','Start_Date_and_Time_of_Call_Setup']].astype('datetime64[ns]')
            

                    
        elif ne.startswith("Source1_Performance"):
            Output_FileName=FileName.replace(FileName[-15:], '.csv') 
            if "_In.csv" in Output_FileName:
                DF.columns = ['Object_Instance_ResultTime','Trunk_Name','SEIZURE_TIMES','TERMINAL_CALL_ATTEMPT_TIMES','TRANSFER_CALL_ATTEMPT_TIMES','CALL_CONNECTED_TIMES','ANSWER_TIMES','TERMINAL_CALL_ANSWER_TIMES','TRANSFER_ANSWER_TIMES','LOST_CALL_INTERNAL_CONGESTION_TIMES','CONGESTION_DURATION','CALLED_BUSY_TIMES','ABANDON_AFTER_RING_TIMES','RINGED_NO_ANSWER_TIMES','INSTALLED_CIRCUIT_NUM','AVAIL_CIRCUIT_NUM','BLOCKED_CIRCUIT_NUM','INSTALLED_BOTHWAY_CIRCUIT_NUM','AVAIL_BOTHWAY_CIRCUIT_NUM','BLOCKED_bi_directional_CIRCUIT_NUM','BIDIRECTION_TK_SEIZURE_TRAFFIC','SEIZURE_TRAFFIC','CONNECTED_TRAFFIC','ANSWER_TRAFFIC','AVERAGE_SEIZURE_DURATION','ABANDON_BEFORE_RING_TIMES','TERMINAL_ERROR_TIMES','INVALID_ADDRESS_TIMES','EMPTY_CODE_TIMES','TERMINAL_UNCOMPATABLE_TIMES','BEARER_CAPABILITY_NOT_PERMIT_TIMES','BEARER_CAPABLILITY_AVAILABLE_TIMES','INCOMPLETE_DIALLING_TIMES','INTK_AVAIL_RATIO','CONNECTED_RATIO','ANSWER_RATIO','BLOCKED_CIRCUIT_RATIO','BLOCKED_bi_directional_CIRCUIT_RATIO','CALL_ATTEMPT_TIMES_IN_PCCW','ANSWER_TIMES_IN_PCCW','PEER_OFFICE_CONGESTION_TIMES','SIP_CIRCUITS','TK_LIC_OVERFLOW_TIMES']
            elif "_Out.csv" in Output_FileName:
                DF.columns = ['Object_Instance_ResultTime','Trunk_Name','BID_TIMES','SEIZURE_TIMES','TRANSFER_BID_TIMES','TRANSFER_SEIZURE_TIMES','CALL_CONNECTED_TIMES','ANSWER_TIMES','OVERFLOW_TIMES','TRUNK_CIRCUIT_MISMATCHED_TIMES','BEARER_CAPABILITY_MISMATCHED_TIMES','DUAL_SEIZURE_TIMES','TK_RETRY_TIMES','PEER_END_CONGESTION_TIMES','CONGESTION_DURATION','CALL_AFFECTED_BY_NET_ADMINISTRATOR_ACTION_TIMES','DIRECT_ROUTE_RESTICTION_CALL_LOSS_TIMES','CIRCUIT_ORIENT_SET_CALL_LOSS_TIMES','CIRCUIT_TEMP_STOP_CALL_LOSS_TIMES','CANCEL_ALTERNATIVE_ROUTE_TIMES','ROUTE_JUMP_TIMES','TEMP_AMB_TIMES','CALLED_BUSY_TIMES','CALLED_TOLL_BUSY_TIMES','CALLED_LOCAL_BUSY_TIMES','ABANDON_AFTER_RING_TIMES','RINGED_NO_ANSWER_TIMES','INSTALLED_CIRCUIT_NUM','AVAIL_CIRCUIT_NUM','BLOCKED_CIRCUIT_NUM','INSTALLED_BOTHWAY_CIRCUIT_NUM','AVAIL_BOTHWAY_CIRCUIT_NUM','BLOCKED_bi_directional_CIRCUIT_NUM','OUTTK_RATIO','SEIZURE_RATIO','CONNECTED_RATIO','ANSWER_RATIO','BLOCKED_CIRCUIT_RATIO','BLOCKED_bi_directional_CIRCUIT_RATIO','BOTHWAY_CIRCUIT_SEIZURE_TRAFFIC','SEIZURE_TRAFFIC','CONNECTED_TRAFFIC','ANSWER_TRAFFIC','AVERAGE_SEIZURE_DURATION','EMPTY_CODE_TIMES','INVALID_ADDRESS_TIMES','BEARER_CAPABILITY_NOT_PERMIT_TIMES','BEARER_CAPABILITY_NOT_LAYOUT_TIMES','BEARER_CAPABLILITY_AVAILABLE_TIMES','TERMINAL_ERROR_TIMES','ABANDON_BEFORE_RING_TIMES','CALLED_BUSY_TIMES_TEMP','INCOMPLETE_DIALLING_TIMES','PEER_OFFICE_CONGESTION_TIMES','SIP_CIRCUITS','SIP_H323_TRUNK_LIMIT_DURATION','SIP_H323_TRUNK_LIMIT_TIMES','LOST_CALL_TIMES','INTERNAL_RELEASE_TIMES','INTERNAL_FAILURE_TIMES']
            elif "_InLet.csv" in Output_FileName or "_OutLet.csv" in Output_FileName:
                DF.columns = ['Object_Instance_ResultTime','Trunk_Name','call_attempt_times','seizure_times','alert_times','answer_times','answer_but_no_billing_times','seizure_time_duration','answer_time_duration','switch_equipment_congestion','no_dialing_in_long_time','no_answer_in_long_time','temporary_failure','no_alerting_in_long_time','continuity_check_failure','exceed_maximum_reattempt_times','release_before_ring','release_before_answer','call_barring','switch_equipment_failure','callee_is_busy_in_a_toll_call','callee_is_busy_in_a_local_call','call_reject_because_of_arrearage','calling_barring','remote_equipment_congestion','call_failure','dual_seizure','invalid_directory_number','callee_arrearage','net_management_barring','adandon_without_dialling','timeout_before_dialling','abandon_with_partial_dialling','timeout_in_dialling_interval','call_barring_because_of_black_and_white_list','calling_party_number_judgement_restriction','callee_is_busy','no_response_from_callee','call_rejected','number_changed','termination_error','invalid_DN_format','normal','no_route_available','no_route_or_circuit_applied_available','restricted_by_one_level_CPU_overload','restricted_by_two_level_CPU_overload','restricted_by_three_level_CPU_overload','restricted_by_four_level_CPU_overload','lost_calls_of_other_reasons','mean_value_of_seizure_time_duration','mean_value_of_answer_time_duration']

        
        Logs_Archiving("File ("+ FileName+ ") has been formated successfully ")
    
        return DF
    except Exception as e:
        Errors_Archiving('Phase format_fields  '+ne +'   '+FileName+'   '+str(e))
        pass


##########################################
def process_pandas_data(func, df, num_processes, cdr_file):
    '''
    :param func:
    :param df:
    :param num_processes:
    :return:
    '''
    if cdr_file.endswith('.dat'):
        with Pool(num_processes) as pool:
            data = [[df[col], return_value(col)] for col in [str(*col) for col in d_map_value]]
            mapped_data = pool.imap(func, data)
            df.loc[:, [str(*col) for col in d_map_value]] = pd.concat(mapped_data, axis=1)
    Logs_Archiving("File ("+ cdr_file+ ") has been mapped successfully ")

    return df
##########################################
def return_value(key):
    for item in d_map_value:
        if str(*item) == key:
            return item[key]
##########################################
def map_values(cdr_records):
    DF = cdr_records[0]
    MAP = cdr_records[1]
    for item in range(len(DF)):
        DF[item] = MAP[DF[item]]
    return DF

##########################################
def direction(pfx):
    if len(pfx) == 9 and pfx[0] == '7':
        return '967'+pfx

    if len(pfx) == 7  :
        return '967' + pfx

    return pfx

##########################################
def map_prefix_call_coutny(prefix, cn='Unknown', cc=d_country_code):
    if prefix == '':
        return 'No CLI'
    
    #to solve conflic between YEM-YMO & YEM-PTC (967-78)
    if (len(prefix) == 7 and prefix[0] == '7') or (len(prefix) == 10 and prefix[0:3] == '967'):
        return 'YEM-PTC'

    prefix = direction(prefix)


    if prefix[:6] in cc.keys():
        return cc[prefix[:6]]
    elif prefix[:5] in cc.keys():
        return cc[prefix[:5]]
    elif prefix[:4] in cc.keys():
        return cc[prefix[:4]]
    elif prefix[:3] in cc.keys():
        return cc[prefix[:3]]
    elif prefix[:2] in cc.keys():
        return cc[prefix[:2]]
    elif prefix[0] in cc.keys():
        return cc[prefix[0]]

    return cn


##########################################
def add_fields(ne, filename,DF):
    '''
    :param cdrs_records:
    :return:
    '''
    try:
        if ".dat" in  filename:
            DF['caller_country'] = DF.apply(lambda row: map_prefix_call_coutny(row.caller_number), axis=1)
            DF['called_country'] = DF.apply(lambda row: map_prefix_call_coutny(row.called_number), axis=1)
            DF['cdr_file_name'] = DF.apply(lambda row: strftime('%m_%d_%Y')+"_"+filename+"0" , axis=1)
        
                       
        Logs_Archiving("File ("+ filename+ ") has been added successfully ")                   
    except Exception as e:
        Errors_Archiving('Phase add_fields  '+ne +'   '+filename+'   '+str(e))
        pass
    
    
    return DF


##########################################
def store_data(ne,filename,cdrs_records,store_para):

    '''
    Input: filename, DF , Server IP, User. Password 
    this function stores DF to Database
    '''
    #print(ne,filename,cdrs_records,store_para)
    if  ne.startswith('Source2_CDR') or ne.startswith('Source1_Performance') or ne.startswith("IGW6"):
        cdrs_records,filename=Check_if_Records_already_stored(ne,cdrs_records,filename)
        #pass
    if len(cdrs_records)>0:
        store_to_Server,DB_User,DB_Password,DB_Name,Table,Schema =store_para.split(';')
        try:
            engine = create_engine("mssql://"+DB_User+':'+ str(DB_Password)+'@'+store_to_Server+"/"+DB_Name+"?driver=SQL+Server+Native+Client+11.0")
            conn = engine.connect()
            cdrs_records.to_sql(Table,schema=Schema, con=conn, if_exists='append', index=False)
            conn.close()
            Logs_Archiving("File ("+ filename+ ") has been stored successfully ")

            
               
        except Exception as e:
            Errors_Archiving('Phase storeing  '+filename+'   '+str(e))

##########################################
def Check_if_Records_already_stored(ne,DF,filename):
    '''
    special func to avoid duplicates 
    '''

    os.chdir('C:/Multisource_Data_Pipeline/Downloaded_Files/')
    if ne.startswith('Source2_CDR'):
        FilesInDB=Check_Data_On_DB(ne, 'CDRs_stored')
        FilesInDB= FilesInDB.to_numpy()
        if strftime('%m_%d_%Y')+"_"+filename in FilesInDB:
            connection_info = 'Driver={SQL Server};Server=localhost;Database=master;Trusted_Connection=yes;'
            query="delete from [CDR_db].[dbo].[CDR_tbl] where [cdr_file_name]='"+strftime('%m_%d_%Y')+"_"+filename+"'"
            Execute_SQL_Query(connection_info,query,"No_Output")


    if ne.startswith('Source1_Performance'):
        filename=filename.replace(filename[-15:], '.csv')
        DateColumnName = "Object_Instance_ResultTime"
        #check last timestamp in the database 
        Last_Imported_Time = Check_Data_On_DB(ne , filename)
        DF=DF[DF['{}'.format(DateColumnName)]>Last_Imported_Time]
        if len(DF)>0:
            DF=DF.drop_duplicates(subset=['ID'],keep=False)
            DF.to_csv(filename, sep=',', index=False)

                
    return DF,filename

##########################################
def Check_Data_On_DB(ne , File):
    '''
    This Function fetches the last date or unique fileNames in sql DB
    '''
    
    variable=''
    try:                  
        sql_conn = pyodbc.connect('Driver={SQL Server};Server=localhost;Database=Performance_Files;Trusted_Connection=yes;')
        if File=='CDRs_stored':
            query="SELECT distinct [cdr_file_name]  FROM [CDR_db].[dbo].[CDR_tbl]"
            Data_From_DB = pd.read_sql(query, sql_conn)
  
               
        
        elif ne.startswith('Source1_Performance'):
            File= File.replace('.csv', '')
            FileInfo= Get_File_Info(ne,File)
            archive_flag,read_para,store_para,archive_para=FileInfo.split('|')
            store_to_Server,DB_User,DB_Password,DB_Name,Table,Schema=store_para.split(';')
            DateColumnName = "Object_Instance_ResultTime"
            #check last timestamp in the database 
            query="SELECT TOP (1) ["+DateColumnName+"]  FROM ["+DB_Name+"].[dbo].["+Table+"] order by ["+DateColumnName+"] desc"
            variable = pd.read_sql(query, sql_conn)
            variable = variable.loc[0][0]
            Data_From_DB  = str(variable).replace('Timestamp(','').replace(')','')


            
    except Exception as e:
        Errors_Archiving(str(e))
        pass
    return Data_From_DB

##########################################
def execute_SQL_Scripts(ne):
    '''
    Input: stored procedure names located in path C:\Multisource_Data_Pipeline\
    Output: run these queries on the sql server
    '''
    # Houly 
    arry=['Sql_Query_Hourly_1','Sql_Query_Hourly_2']

    for string in arry:
        Execute_CMD("sqlcmd -s 127.0.0.1 -E -i C:\\Multisource_Data_Pipeline\\"+string+".sql > C:\\Multisource_Data_Pipeline\\SQL_Output_Result.txt")
    
    #Daily, if need to perform the sql query only once a day at specific hour like 06 AM
    Hour_Of_Now=str(datetime.strftime(datetime.now(), '%H:'))
    if Hour_Of_Now =='06:':
        Execute_CMD("sqlcmd -s 127.0.0.1 -E -i C:\\Multisource_Data_Pipeline\\Sql_Query_Daily.sql >> C:\\Multisource_Data_Pipeline\\SQL_Output_Result.txt")

    #The >> is indeed a shell redirection operator that appends the output of the SQL command to the specified file,
    #SQL_Output_Result.txt, Therefore, the output of the SQL command will be appended to the file if it exists,
    #or a new file will be created if it doesn't exist         

##########################################    
def Execute_SQL_Query(connection_info,query,query_type):
    '''
    Execute all kinds of SQL Queries (insert or update or truncate)
    '''
    
    try:
        if query_type=="No_Output":
            sql_conn = pyodbc.connect(connection_info)
            cursor = sql_conn.cursor()
            cursor.execute(query)
            cursor.commit()
      
    except Exception as e:
        Errors_Archiving(str(e))
        pass

##########################################
def Execute_Many_SQL_Query(connection_info,query,records):
    cnxn = pyodbc.connect(connection_info)
    cursor = cnxn.cursor()
    try:
        cursor.executemany(query, records)
        cnxn.commit()
        cursor.close()
        cnxn.close()
    except Exception as e:
        print(e)
        cnxn.commit()
        cursor.close()
        cnxn.close()


##########################################  
def Execute_CMD(cmd):
    '''
    this fun executes the needed CMD Commands
    '''
    print(cmd)
    return os.system(cmd)


##########################################       
def Archive_Files(ne, filename, archive_para):
    '''
    Archive the needed input File and delete others
    '''
    os.chdir('C:/Multisource_Data_Pipeline/Downloaded_Files/')

    if archive_para=="delete":
        os.remove(filename)
    else:
        shutil.move(filename ,archive_para+ filename )
    
    
   
##########################################            
def Errors_Archiving(log_error):
    '''
    this func archives the executing Errors.
    '''
    print(log_error)
    Output_Writer = open('C:/Multisource_Data_Pipeline/ErrorFile.txt', 'a')
    Output_Writer.write(strftime('%d_%m_%Y %H:%M:%S')+'   '+log_error+'\n')
    Output_Writer.close()
##########################################  
def Logs_Archiving(log_line):
    '''
    this func archives the executing LOGs.
    '''
    print(log_line)
    Output_Writer = open('C:/Multisource_Data_Pipeline/logFile.txt', 'a')
    Output_Writer.write(strftime('%d_%m_%Y %H:%M:%S')+' '+log_line+'\n')
    Output_Writer.close()
##########################################  
def NE_Input_Para(neFile):
    '''
    this new func reads the input Networ Element parameters
    '''
    NeData = {}
    for i in range(len(neFile)): 
        NeData[neFile['Network_Element'][i]] = neFile['Local_IP'][i],neFile['Remote_IP'][i],neFile['Remote_HostName'][i], neFile['Protocol'][i], neFile['Username'][i], neFile['Password'][i], \
                                           neFile['Shared_Folder1'][i], neFile['Shared_Folder2'][i], neFile['Path'][i], neFile['FileType'][i], neFile['L_dir'][i] ,neFile['Last_DownloadedFile'][i]
    return NeData



##########################################
'''
Below functions are no longer used
'''
##########################################
def Unzip_File(path,filename,FileType):
    '''
    this func unzips the compressed files
    '''
    Unziped_File_Name=""
    if ".zip" in FileType:
        shutil.unpack_archive(path+'/'+filename, path, "zip")
        if filename.startswith("Summary CDR Archive Report"):
            return filename.replace('.zip','.xlsx')
        else:
            return filename.replace('.zip','.csv')
    elif ".gz" in FileType:
        Unziped_File_Name=filename.replace('.csv.gz','.csv')
        f_out=open(path+'/'+Unziped_File_Name,'w')
        with gzip.open(path+'/'+filename) as f:
            f_out.write(f.read().decode("utf-8"))
            f_out.close()
            f.close()
            return Unziped_File_Name
    else:
        return filename

##########################################
def change_epoch_time(input_epoch_time):
    '''
    change_epoch_time into normal date time
    Input: local date time to be fetched from db
    Output: epoch time

    '''
    try:
        result= time.strftime("%m-%d-%Y %H:%M:%S",time.localtime(float(input_epoch_time)))
    
        return result
    except Exception as e:
        result=''
        return result
    
