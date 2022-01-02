# UserTransactions_DataPipeline
<h2>Objective</h2>

<b>To solve the some of the Use case of the Data PipeLine and <i>Implement the SCD1 in Hive</i> </b>

<h3><b>Scenario</b></h3>

<img src="https://github.com/melwinmpk/UserTransactions_DataPipeline/blob/main/img/autodraw%201_2_2022.png?raw=true">

<p>You are given Data that is stored in multiple CSV files (its about the User Transactions)</p>
<ol>
    <li>load the data from the CSV file to the SQL table.</li>
    <li>Using Sqoop Job (Incremental Load) send the data from the SQL to hdfs.</li>
    <li>Using Hive first load the Data to the Manage table then load the data to the External Table 
        <b><i>Implement the SCD 1</i></b> in this process. </li>
    <li>Finally, load back the data to another SQL table to cross verify the data from the Source to Destination</li>
</ol>

<h3><b>Input :</b></h3>
<p>given three csv's stored in this format</p>


<p>
<i><b>"custid","username","quote_count","ip","entry_time","prp_1","prp_2","prp_3","ms","http_type","purchase_category",</i></b>
<b><i>"total_count","purchase_sub_category","http_info","status_code"</b></i>
</p>
