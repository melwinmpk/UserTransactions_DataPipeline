#! /bin/bash

log_dir=/home/saif/LFS/logs/
basefile=`basename ${0}`
currdate=`date +"%Y-%m-%d_%T"`
logfile=${log_dir}${basefile}_${currdate}.log

if [ $# -ne 2 ]
then
        echo "Missing Sourcefile or Python script file"
        echo "Missing Sourcefile or Python script file" >> ${logfile}
        exit 1
fi
echo "Connecting to the MY SQL TO Create table"


mysql -u root -pWelcome@123 retail_db<<EOFMYSQL

DROP TABLE IF EXISTS cus_transactions;

CREATE TABLE cus_transactions(
custid INT,
username VARCHAR(125),quote_count INT,
ip VARCHAR(125),
entry_time VARCHAR(125),
prp_1 INT,
prp_2 INT,
prp_3 INT,
ms VARCHAR(20),
http_type VARCHAR(20),
purchase_category VARCHAR(525),
total_count INT,
purchase_sub_category VARCHAR(525),
http_info VARCHAR(500),
status_code INT,
Year INT DEFAULT -1,
Month INT DEFAULT -1
);
DROP TABLE IF EXISTS cus_transactions_check;

CREATE TABLE cus_transactions_check(
custid INT,
username VARCHAR(125),quote_count INT,
ip VARCHAR(125),
entry_time VARCHAR(125),
prp_1 INT,
prp_2 INT,
prp_3 INT,
ms VARCHAR(20),
http_type VARCHAR(20),
purchase_category VARCHAR(525),
total_count INT,
purchase_sub_category VARCHAR(525),
http_info VARCHAR(500),
status_code INT,
Year INT DEFAULT -1,
Month INT DEFAULT -1
);

DROP TABLE IF EXISTS cus_transactions_export;

CREATE TABLE cus_transactions_export(
custid INT,
username VARCHAR(125),quote_count INT,
ip VARCHAR(125),
entry_time VARCHAR(125),
prp_1 INT,
prp_2 INT,
prp_3 INT,
ms VARCHAR(20),
http_type VARCHAR(20),
purchase_category VARCHAR(525),
total_count INT,
purchase_sub_category VARCHAR(525),
http_info VARCHAR(500),
status_code INT,
Year INT DEFAULT -1,
Month INT DEFAULT -1
);
EOFMYSQL

if [ $? -ne 0 ]
then
	echo "Table did not get succesfully Please check !!!"
	echo "Table did not get succesfully Please check !!!" >> ${logfile}
	exit 1
else
	echo "Teable created succesfully!!"
fi

# loading of the data
python3 ${2} ${1}   # csv_to_sql.py /home/saif/LFS/cohart-8/assignments/Assignments24th/1.csv

if [ $? -ne 0 ]
then
	echo "Data did not load to the database !!!"
	echo "Data did not load to the database !!!" >> ${logfile}
	exit 1
else
	echo "Loading of the Data succesfully!!"
fi

mysql -u root -pWelcome@123 retail_db<<EOFMYSQL
update cus_transactions
set Year = SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",-1),
    Month = CASE
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "JAN" THEN 01
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "FEB" THEN 02
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "MAR" THEN 03
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "APR" THEN 04
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "MAY" THEN 05
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "JUN" THEN 06
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "JUL" THEN 07
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "AUG" THEN 08
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "SEP" THEN 09
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "OCT" THEN 10
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "NOV" THEN 11
        WHEN UPPER(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",2),"/",-1)) = "DEC" THEN 12
    END
    WHERE Year = -1;
EOFMYSQL



# deleting the

hdfs dfs -ls HFS/Input/cus_transactions

if [ $? -eq 0 ]
then
	echo "Deleting the HDFS folder"
	hdfs dfs -rm -r HFS/Input/cus_transactions
	if [ $? -eq 0 ]
	then
		echo "Delete Successfull"
	fi
fi




echo "Creating the sqoop job"
sqoop job --delete inc_lastmodified_transactions
# YYYY-MM-DD HH:MM:SS.fffffffff
sqoop job --create inc_lastmodified_transactions -- import \
--connect jdbc:mysql://localhost:3306/retail_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--query "SELECT custid, username, quote_count, ip, entry_time, prp_1, prp_2, prp_3, ms, http_type, purchase_category,
        total_count,purchase_sub_category, http_info, status_code, Year, Month   FROM cus_transactions where \$CONDITIONS" \
-m 1 \
--target-dir /user/saif/HFS/Input/cus_transactions \
--append



sqoop job --exec inc_lastmodified_transactions



if [ $? -eq 0 ]
then
	echo "Sqoop Job creation successfull"
else
  echo "Sqoop Job did not  creation successfull"
  echo "Sqoop Job did not  creation successfull" >> ${logfile}
  exit 1
fi



hdfs dfs -rm -r /user/hive/warehouse/cus_transactions_external
hdfs dfs -rm -r /user/hive/warehouse/cus_transactions_process_manage

hive -e "
        set hive.support.concurrency=true;
        set hive.enforce.bucketing=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.compactor.initiator.on=true;
        set hive.compactor.worker.threads=1;
        set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.exec.max.dynamic.partitions=100;
        set hive.exec.max.dynamic.partitions.pernode=100;

        Set hive.auto.convert.join=false;
        Set hive.merge.cardinality.check=false;


        drop table cus_transactions_manage;
        create table if not exists cus_transactions_manage
        (
        custid INT,
        username string,
        quote_count INT,
        ip string,
        entry_time string,
        prp_1 int,
        prp_2 int,
        prp_3 int,
        ms string,
        http_type string,
        purchase_category string,
        total_count int,
        purchase_sub_category string,
        http_info string,
        status_code int,
        Year int,
        Month int
        )
        row format delimited fields terminated by ','
        stored as textfile;
        load data inpath '/user/saif/HFS/Input/cus_transactions' into table cus_transactions_manage;

        drop table cus_transactions_process_manage;
        create table if not exists cus_transactions_process_manage
        (
          custid INT,
          username string,
          quote_count INT,
          ip string,
          entry_time string,
          prp_1 int,
          prp_2 int,
          prp_3 int,
          ms string,
          http_type string,
          purchase_category string,
          total_count int,
          purchase_sub_category string,
          http_info string,
          status_code int
        )
        PARTITIONED BY(Year int, Month int)
        row format delimited fields terminated by ','
        lines terminated by '\n'
        stored as orc
        tblproperties('transactional'='true');

        MERGE INTO cus_transactions_process_manage
        USING cus_transactions_manage z
        ON cus_transactions_process_manage.custid=z.custid
        WHEN MATCHED THEN
        UPDATE SET
        custid = z.custid,
        username = z.username ,
        quote_count = z.quote_count  ,
        ip = z.ip,
        entry_time = z.entry_time,
        prp_1 = z.prp_1,
        prp_2 = z.prp_2,
        prp_3 = z.prp_3,
        ms = z.ms,
        http_type = z.http_type,
        purchase_category = z.purchase_category ,
        total_count = z.total_count,
        purchase_sub_category = z.purchase_sub_category,
        http_info = z.http_info,
        status_code = z.status_code
        WHEN NOT MATCHED THEN
        INSERT VALUES (
          z.custid, z.username, z.quote_count, z.ip, z.entry_time, z.prp_1, z.prp_2, z.prp_3, z.ms, z.http_type, z.purchase_category,
          z.total_count, z.purchase_sub_category, z.http_info, z.status_code, z.Year, z.Month
        );


        drop table cus_transactions_external;
        create external table if not exists cus_transactions_external
        (
          custid INT,
          username string,
          quote_count INT,
          ip string,
          entry_time string,
          prp_1 int,
          prp_2 int,
          prp_3 int,
          ms string,
          http_type string,
          purchase_category string,
          total_count int,
          purchase_sub_category string,
          http_info string,
          status_code int
        )
        PARTITIONED BY(Year int, Month int)
        row format delimited fields terminated by ','
        lines terminated by '\n';

        insert overwrite table cus_transactions_external select * from cus_transactions_process_manage;



        "
if [ $? -eq 0 ]
then
	echo "Hive manage table created and loaded the data"
fi

echo "Creating the sqoop export job"
sqoop job --delete inc_lastmodified_transactions_export

sqoop job --create inc_lastmodified_transactions_export -- export \
--connect jdbc:mysql://localhost:3306/retail_db?useSSL=false \
--username root --password-file file:///home/saif/LFS/datasets/sqoop.pwd \
--table cus_transactions_export \
--export-dir /user/hive/warehouse/cus_transactions_manage \
--m 1 \
-- driver com.mysql.jdbc.Driver \
--input-fields-terminated-by ',' \
--append

sqoop job --exec inc_lastmodified_transactions_export

