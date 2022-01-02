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
mysql -u root -pWelcome@123 retail_db<<EOFMYSQL
  truncate TABLE cus_transactions;
EOFMYSQL

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
set
    Year = SUBSTRING_INDEX(SUBSTRING_INDEX(entry_time,":",1),"/",-1),
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
sqoop job --exec inc_lastmodified_transactions



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

        truncate TABLE cus_transactions_manage;

        load data inpath '/user/saif/HFS/Input/cus_transactions' into table cus_transactions_manage;

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

        insert overwrite table cus_transactions_external select * from cus_transactions_process_manage;



        "
if [ $? -eq 0 ]
then
	echo "Hive manage table created and loaded the data"
fi

sqoop job --exec inc_lastmodified_transactions_export