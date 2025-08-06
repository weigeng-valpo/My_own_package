import pandas as pd
import pyodbc
import ibm_db as db
import ibm_db_dbi
from tqdm import tqdm
from snp_query_box.select_lists import *
import warnings

class ServerConnection:
    """This class establishes a connection to Server"""

    def medcompass_connection(self):
        conn=pyodbc.connect("DSN=hiveldap.aetna.com", autocommit=True)
        cursor = conn.cursor
        return conn, cursor
    
    def db2_connection(self, USERNAME, PASSWORD):
        conn_handle = db.connect("DATABASE=DEWHP000;"
              "HOSTNAME=ewhprd.iwhouse.aetna.com;"
             "PORT=10000;"
             "PROTOCOL=TCPIP;"
              "UID={};".format(USERNAME)+""
              "PWD={};".format(PASSWORD), "", "")
        conn = ibm_db_dbi.Connection(conn_handle) 
        cursor = conn.cursor
        return conn, cursor
    
    def stars_sop_rpt_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                    'Server=STARSRPT;'
                    'Database=starssoprpt;'
                    'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor
    
    def stars_sales_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                        'Server=MEPRPRDDB;'
                        'Database=MEPRBI;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor

    def stars_bi_data_prod_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                        'Server=MDCRBIPRODDB;'
                        'Database=StarsBIDataProd;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor

    def stars_data_hub_prod_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                        'Server=StarsDataengineProddb;'
                        'Database=StarsDataHubProd;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor

    def qnxt_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                            'Server=AMDPRODALT;'
                            'Database=Duals_Prod;'
                            'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn,cursor

    def qnxt_nj_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                    'Server=SRVQNXTRPTNJPROD;'
                    'Database=COE_FIDE_RPT;'
                    'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor
        
    def qnxt_va_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                        'Server=SRVQNXTRPTVAPROD;'
                        'Database=VA_DSNP_RPT;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor
    
    def qnxt_ca_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                        'Server=SRVQNXTRPTCAPROD;'
                        'Database=CA_EAE_TEMP;'
                        'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor
    
    def amdprodcoe01_connection(self, driver = '{ODBC Driver 17 for SQL Server}'):
        conn=pyodbc.connect(f'Driver={driver};'
                    'Server=AMDPRODCOE01;'
                    'Database=Duals_Prod;'
                    'Trusted_Connection=yes;')
        cursor = conn.cursor
        return conn, cursor