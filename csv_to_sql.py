import csv
import logging
import mysql.connector as connection
import sys
from datetime import datetime as dt

logging.basicConfig(filename="logdata24thDec.txt")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class load_data_to_sql:

    def __init__(self, data_path):
        self.path = data_path

    def sqlconnect(self):
        self.mydb = connection.connect(host="localhost", database="retail_db",
                                       user="root", passwd="Welcome@123", use_pure=True)
        self.mydb_cursor = self.mydb.cursor()

    def writedata_to_sql(self):
        self.sqlconnect()
        rows = ""
        itemrating = {}

        with open(f'{self.path}', mode='r', newline="") as csvfile:
            header = [
                        'custid',
                        'username',
                        'quote_count',
                        'ip',
                        'entry_time',
                        'prp_1',
                        'prp_2',
                        'prp_3',
                        'ms',
                        'http_type',
                        'purchase_category',
                        'total_count',
                        'purchase_sub_category',
                        'http_info',
                        'status_code',
                     ]
            reader = csv.reader(csvfile)
            rows = ""

            for row in reader:
                rows += f"""('{row[0]}','{row[1]}','{int("".join((row[2].split(" "))))}','{row[3]}','{row[4]}',    
                             '{row[5]}','{row[6]}','{row[7]}','{row[8]}','{row[9]}',
                             '{row[10]}','{row[11]}','{row[12]}',"{row[13]}",'{row[14]}'),"""

            rows = rows[:-1]
            query = f"""
                          INSERT INTO cus_transactions 
                            ( custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, 
                            purchase_category, total_count, purchase_sub_category, http_info, status_code) VALUES {rows};
                    """
            self.mydb_cursor.execute(query)
            query = f"""
                                      INSERT INTO cus_transactions_check 
                                        ( custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, 
                                        purchase_category, total_count, purchase_sub_category, http_info, status_code) VALUES {rows};
                                """
            self.mydb_cursor.execute(query)

        self.mydb.commit()
        self.mydb.close()



if __name__ == '__main__':
    object = load_data_to_sql(sys.argv[1])
    object.writedata_to_sql()
    try:
        print("what is this")
    except Exception as e:
        logger.error(f"Oops! {e.__class__} occurred.")
    else:
        logger.info("Writing Data to sql Executed Successfully")