from doctest import REPORT_ONLY_FIRST_FAILURE
from datetime import date, datetime, timedelta

import paramiko
import json
import pyodbc
import pandas as pd
import os
import sys
import smtplib

"""  Get the initialization parameters.    """

# If the name isn't passed as argv[1] then use the json named the same as the app. 

# OS module knows whether to use NT or Posix file naming schemes.  

if len(sys.argv) == 1:
    init_filename = os.path.basename (sys.argv[0])
    ext_dot_position = init_filename.find ('.')
    if ext_dot_position > 0:
        init_filename = init_filename = os.path.join (os.path.dirname(sys.argv[0]),init_filename [:ext_dot_position] + '.json')
else:
    init_filename = sys.argv[1]


"""  Initialization """

email_header_set = False

with open(init_filename, 'r', encoding='utf-8', newline='') as init_file:
    init_params = json.load (init_file)

#  Build the connection string for the Sql Server database 
connection_string = 'DRIVER={ODBC Driver 17 for SQL Server}'\
    + ';SERVER=' + init_params["server"] \
    + ';DATABASE=' + init_params["db_name"] \
    + ';UID=' + init_params["sql_login"] \
    + ';PWD=' + init_params["sql_password"]

#  Set some switches for testing
download_new_files = bool (init_params["do_file_download_bool"] == 'true')
email_recipients = bool (init_params["do_email_distrib_bool"] == 'true')

"""   Define functions  """

#  NOT USED
def mk_file_name_from_date (date_str):
    # Use the 8 digit date to construct the name of the file in the bank's format
    return (init_params["bank_cust_file_prefix"] + date_str[4:] + date_str[2:4] + init_params["bank_cust_file_ext"])  

#  NOT USED
def mk_lookback_date_str (num_lookback_days):                  
    # Return the 8 digit date that's N days ago from today
    return (f"{(datetime.today() - timedelta (days=num_lookback_days)):%Y%m%d}")

# NOT USED
def increment_date (date8):  
    # Return the 8 digit date that's one day later than the date passed in
    date_obj = (datetime.strptime(date8, '%Y%m%d') + timedelta (days=1))  # Convert to DT obj to add a day.
    return (date_obj.strftime('%Y%m%d'))


def mk_file_date_from_file_name (file_name):
    # File name is in three period separated parts with the second being the date in the form MMDDYY.
    # Function reforms the date into YYYYMMDD

    token_list = file_name.split ('.')
    return (datetime.strptime (token_list[1], '%m%d%y')).strftime ('%Y%m%d')


def download_bank_files ():
    # Download all files found on server and return the list of files 

    paramiko.util.log_to_file(init_params['log_file'], level = init_params['log_level'])

    k = paramiko.RSAKey.from_private_key_file(init_params['private_key_file'])
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    connect_params = {
        'hostname': init_params['bank_host_name'],
        'username': init_params['bank_username'],
        'pkey': k,
        'disabled_algorithms': {
            'pubkeys': ['rsa-sha2-512', 'rsa-sha2-256']
        }
    }

    c.connect(**connect_params)

    transport = c.get_transport()

    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.chdir (init_params["bank_dir"])

    host_file_list = (sftp.listdir())
 
    local_file_list =[]

    for file_to_get in host_file_list:
 
        print (file_to_get)
        local_file_name = init_params["local_get_path"] + os.path.basename (file_to_get)

        sftp.get (file_to_get, local_file_name)
        #  I'll want a return code and a try-except block
        local_file_list.append (local_file_name)

    c.close()

    return local_file_list


def get_local_files ():
    
    os.chdir (init_params["local_get_path"])
    return (os.listdir())


def update_partner_table (date8, file_name):
    
    """ Inserts a Partner row and marks the file as succesfully downloaded"""
 
    cnxn = pyodbc.connect (connection_string)
    
    cursor = cnxn.cursor()
    
    col_vbls = (init_params["tbl_row_type_indicator"], date8, init_params["file_sn_const"],\
         os.path.basename (file_name), init_params["prt_code_const"], datetime.now())
    
    data_frame = []
    data_frame.append (list (col_vbls))
    table = pd.DataFrame.from_records (data_frame)

    sql_statement = 'insert into INRHub.dbo.partner_tx_status\
         ([Prt_Tx_Type_Code],[File_Date],[File_SN],[File_Name],[Partner_Code],[Tx_Success_DT]) '\
    + 'values (?,?,?,?,?,?)'

    cursor.executemany (sql_statement, table.values.tolist())
    cnxn.commit()
    cnxn.close()

    return


def parse_bai_file (bai_file):

    """ Reads through a comma separated BAI file to extract each account number, ending balance and 
        as-of date-time into a list of lists """

    dly_report = []

    with open (bai_file, 'r', encoding='utf-8') as bai_data:
        txt_line = bai_data.readline()

        while txt_line:
            token_list = txt_line.split(',')
         
            if token_list[0] == '02':
                asof_date = datetime.strptime (token_list[4] + token_list[5], '%y%m%d%H%M')

            if token_list[0] == '03':
                acct_num = token_list[1]
                close_bal = float (token_list[8]) / 100
               
                acct_row = []
                acct_row.append (acct_num)
                acct_row.append (close_bal)
                acct_row.append (asof_date)
                acct_row.append (os.path.basename (bai_file))

                dly_report.append (acct_row)              

            txt_line = bai_data.readline ()
        
        bai_data.close ()

    return (dly_report)


def check_for_partner_row (date8):

    #  Queries the Partner Tx table for a row with date8 as file date

    try:
        cnxn = pyodbc.connect (connection_string)
        cursor = cnxn.cursor()
   
        sql_statement = 'select count(*) from inrhub.dbo.Partner_Tx_Status \
            where File_date = ? and file_sn = ? and partner_code = ?\
                and tx_success_dt is not null and tx_canceled_dt is null'

        match_vbls = [date8, init_params["file_sn_const"], init_params["prt_code_const"]]

        df = pd.DataFrame (cursor.execute (sql_statement, match_vbls).fetchall())

        count = df.values.tolist()  
    
        return (bool (count[0][0][0] > 0))  # df is a list w/in a list w/in a list

    except pyodbc.OperationalError:
        print ("Unable to establish connection to database")
    except Exception:
        print ("Sorry, something went wrong with the query on Partner_Tx_Status")

    return (False)


def update_partner_acct_balance_table (acct_list, file_date):

    # Iterate through the list of accounts and update rows with acct balances

    cnxn = pyodbc.connect (connection_string)    
    cursor = cnxn.cursor()
    
    for acct in acct_list:

        col_vbls = (init_params["prt_code_const"], acct[0], init_params["balance_type"], \
            acct[1], acct[2], file_date, init_params["file_sn_const"])

        data_frame = []
        data_frame.append (list (col_vbls))
        table = pd.DataFrame.from_records (data_frame)

        sql_statement = 'insert into INRHub.dbo.partner_acct_balance\
            ([Inst_Code],[Acct_Num],[Bal_Type],[Acct_Bal],[AsOf_DT],[File_Date],[File_SN]) '\
        + 'values (?,?,?,?,?,?,?)'

        cursor.executemany (sql_statement, table.values.tolist())
        
    cnxn.commit()
    cnxn.close()
        
    return


def get_unmailed_account_rows ():

    # Return a data frame of acct records and dates that haven't been mailed.
    cnxn = pyodbc.connect (connection_string)
    cursor = cnxn.cursor()
   
    #  ? = BOKF, ? = Closing Bal, ? = ACCTBAL
    sql_statement = 'select bal.File_Date, bal.File_SN, bal.acct_num, bal.acct_bal, bal.AsOf_DT \
        FROM INRhub.dbo.Partner_Tx_Status stat, INRHub.dbo.Partner_Acct_Balance bal \
            where Process_Success_DT is null \
            and bal.inst_code = ? \
            and bal.bal_type = ? \
            and bal.File_Date = stat.File_Date \
            and bal.file_sn = stat.File_SN \
            and stat.Partner_Code = bal.inst_code \
            and stat.Prt_Tx_Type_Code = ? \
            and stat.TX_Canceled_DT is null \
            and stat.Process_Success_DT is null \
            order by 1, 2'

    match_vbls = [init_params["prt_code_const"],init_params["balance_type"],
                init_params["tbl_row_type_indicator"]]

    df = pd.DataFrame (cursor.execute (sql_statement, match_vbls).fetchall())
    cnxn.close()

    return (df)


def update_partner_process_success (date8):
    
    """ updates the Partner row and marks the email process as succesful"""
 
    cnxn = pyodbc.connect (connection_string)
    
    cursor = cnxn.cursor()
    
    col_vbls = (init_params["tbl_row_type_indicator"], date8, init_params["file_sn_const"],\
         init_params["prt_code_const"])
    
    data_frame = []
    data_frame.append (list (col_vbls))
    table = pd.DataFrame.from_records (data_frame)

    sql_statement = 'update INRHub.dbo.partner_tx_status\
         set [Process_Success_DT] = current_timestamp where\
         ([Prt_Tx_Type_Code] = ? and [File_Date] = ? and [File_SN] = ? and [Partner_Code] = ?)'

    cursor.executemany (sql_statement, table.values.tolist())
    cnxn.commit()
    cnxn.close()

    return


def send_smtp_mail_gmail (msg):  # For testing.  Will delete in future versions.

    if not email_recipients: return  # For testing without sending repeated emails.

    distrib_list = (init_params["mail_distrib"]).split(',')

    sender = init_params ["test_mail_sender"]
    subj = init_params ["mail_subject"]
    msg = "Subject: %s\n%s" % (subj, msg)

    username = init_params ["test_mail_sender"]
    password = init_params ["test_mail_pswd"]

    print ("Distributed to:")

    for rcvr in distrib_list:
    
        smtplib_obj = smtplib.SMTP ("smtp.gmail.com", 587) 
        smtplib_obj.ehlo ()
        smtplib_obj.starttls ()
        smtplib_obj.login (username,password)
        smtplib_obj.sendmail (sender, rcvr.strip(), msg)
        smtplib_obj.quit ()

        print (rcvr)

    return


def send_smtp_mail (msg):

    if not email_recipients: return  # For testing without sending repeated emails.

    distrib_list = (init_params["mail_distrib"]).split(',')

    sender = init_params ["mail_sender"]
    subj = init_params ["mail_subject"]
    msg = "Subject: %s\n%s" % (subj, msg)

    username = init_params ["mail_sender"]
    password = init_params ["mail_pswd"]

    print ("Distributed to:")

    for rcvr in distrib_list:
    
        smtplib_obj = smtplib.SMTP ('smtp.office365.com', 587) 
        smtplib_obj.ehlo ()
        smtplib_obj.starttls ()
        smtplib_obj.login (username,password)
        smtplib_obj.sendmail (sender, rcvr.strip(), msg)
        smtplib_obj.quit ()

        print (rcvr)

    return

def mail_report (acct_table):

    #  Form the body of a text message from the report list and distribute by email
    msg_body = "\nReport for %s\n\n" % (f"{datetime.today():%A %B %d %Y %I:%M %p}" )\
        + "    Account               As-of Time                 Closing Ledger\n"\
        + "  --------------       --------------------------       --------------------\n"

    num_of_acct_rows = 0
    prev_file_date = ''
    prev_file_sn = 0

    for acct_list in acct_table:

        num_of_acct_rows += 1
        acct = acct_list[0]

        file_date = acct[0]
        file_sn = acct[1]
    
        if file_date != prev_file_date or file_sn != prev_file_sn:
            #  Stamp the success date in the partner table
            update_partner_process_success (file_date)

            #  Add an extra blank line to separate the dates
            msg_body += "\n"
            prev_file_date = file_date
            prev_file_sn = file_sn
        
        asof_date = f"{acct[4]:%x %I:%M %p}"
        acct_bal = f"${acct[3]:,.2f}"
        msg_body += "  %s     %s   %s\n" % (acct[2], asof_date, f"{acct_bal:>20}")

    # Stamp the last process_success_dt for the file_date

    if num_of_acct_rows > 0:
        update_partner_process_success (file_date)
        print (msg_body)
        send_smtp_mail (msg_body)
    else:
        print ("No Acct Rows")

    # Figure out what to return
    
    return (0)


#======================= Main =======================

if download_new_files: download_bank_files()

bank_file_list = get_local_files ()

for curr_bank_file in bank_file_list:
 
    curr_file_date8 = mk_file_date_from_file_name (curr_bank_file)

    # check if file has been previously parsed and distributed
    if not check_for_partner_row (curr_file_date8):

        update_partner_table (curr_file_date8, curr_bank_file)
        acct_list = parse_bai_file (curr_bank_file)
        update_partner_acct_balance_table (acct_list, curr_file_date8)

df = get_unmailed_account_rows()

emailStat = mail_report (df.values)

# Clean up the tmp space if switch TBD says so