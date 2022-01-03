# UserTransactions_DataPipeline
<h2>Objective</h2>

<b>To solve the some of the Use case of the Data PipeLine and <i>Implement the SCD1 in Hive</i> </b>

<h3><b>Scenario</b></h3>

<img src="https://github.com/melwinmpk/UserTransactions_DataPipeline/blob/main/img/autodraw%201_2_2022.png?raw=true">

<p>You are given Data that is stored in multiple CSV files (its about the User Transactions)</p>
<ol>
    <li>load the data from the CSV file to the SQL table.</li>
    <li>Using Sqoop Job (Incremental Load) send the data from the SQL to hdfs.</li>
    <li>Using Hive first load the Data from hdfs to the Manage table then load the data to the External Table partition <b>Year wise and then Month wise</b> 
        <b><i>Implement the SCD 1</i></b> in this process. </li>
    <li>Finally, load back the data to another SQL table to cross verify the data from the Source to Destination</li>
</ol>

<h3><b>Input :</b></h3>
<p>given three csv's stored in this format</p>


<p>
<i><b>"custid","username","quote_count","ip","entry_time","prp_1","prp_2","prp_3","ms","http_type","purchase_category",</i></b>
<b><i>"total_count","purchase_sub_category","http_info","status_code"</b></i>
</p>

<h3><b>My Approach :</b></h3>
<ul>
<li>Using Python load the Data to the SQL table store it in the two tables one for the validation and another for shifting the data from the SQL to the HDFS (sqoop job)</li>
<li>Using the Sqoop Job load the data from the SQL table to the HDFS</li>
<li>In the Hive create two manage tables<br>One for loading each CSV file and truncating it after shifting the data. <br>another to store entire data having the SCD1 implementation (as we cannot apply the Acid property to the External table) </li>
<li>Override the Manage Table that had SCD1 got implemented to the External table</li>
<li>Load the data from the Manage that keeps truncating for every file</li>
</ul>
