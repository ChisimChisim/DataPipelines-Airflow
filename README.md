<h1>Data Pipelines with Airflow for a startup called Sparkify</h1>

<h2>1. purpose of this project</h2>

<p>The purpose of this project is to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.  The data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.
The source data resides in S3 and needs to be processed in the data warehouse in Amazon Redshift. The source datasets consist of CSV logs that tell about user activity in the application and JSON metadata about the songs the users listen to.</p>

<h2>2. Datasets in S3</h2>
<p>Song data: s3://udacity-dend/song_data</p>
<p>Log data: s3://udacity-dend/Log_data</p>

<h2>3. DAG in Airflow UI</h2>
<img src="images/example-dag.png" >

<h2>4. Configuring the DAG</h2>
<p>In the DAG, add default parameters according to these guidelines</p>
<ul>
   <li>The DAG does not have dependencies on past runs</li>
   <li>On failure, the task are retried 3 times</li>
   <li>Retries happen every 5 minutes</li>
   <li>Catchup is turned off</li> 
   <li>Do not email on retry</li>
</ul>   

<h2>5. Operators</h2>
<h3>1. Stage Operator</h3>
<p>The stage operator is expected to be able to load any JSON and CSV formatted files from S3 to Amazon Redshift. 
The operator createsand runs a SQL COPY statement based on the parameters provided. 
The operator is containing a templated field that allows it to load timestamped 
files from S3 based on the execution time and run backfills.</p>

<h3>2. Fact Operator</h3>
<p>The fact operator load data from stage tables and run transformations.
Fact tables are usually so massive that they should only allow append type functionality.</p>

<h3>3. Dimension Operator</h3>
<p>The dimention operator load data from stage tables and run transformations.
Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load.
So the operator allows switching between insert modes when loading dimensions.</p>
            
<h3>4. Data Quality Operator</h3>
<p>The final operator to create is the data quality operator, which is used to run checks on the data itself. 
The operator's main functionality is to receive one or more SQL based test cases along with the expected results 
and execute the tests. For each the test, the test result and expected result needs to be checked and 
if there is no match, the operator raise an exception and the task should retry and fail eventually.</p>


<h2>6. datacase schema design and ETL pipeline</h2>
This database is star schema that simplifies business reporting and implements fast aggregation for this analysis.

<h4>Fact Table</h4>
<ol>
      <li><strong>songplays</strong> - records in log data associated with song plays i.e. records with page NextSong
            <ul>
                  <li>songplay_id </li>
                  <li>start_time </li>
                  <li>user_id </li>
                  <li>level</li>
                  <li>user_id </li>
                  <li>song_id</li>
                  <li>artist_id</li>
                  <li>session_id</li>
                  <li>location</li>
                  <li>user_agent</li>
            </ul>
      </li>
</ol>

<h4>Dimension Tables</h4>
<ol>
      <li><strong>users</strong> - users in the app
            <ul>
                  <li>user_id </li>
                  <li>first_name</li>
                  <li>last_name</li>
                  <li>gender</li>
                  <li>level</li>
            </ul>
      </li>
      <li><strong>songs</strong> - songs in music database
            <ul>
                  <li>song_id</li>
                  <li>title</li>
                  <li>artist_id</li>
                  <li>year</li>
                  <li>duration</li>
            </ul>
      </li>
      <li><strong>artists</strong> - artists in music database
            <ul>
                  <li>artist_id </li>
                  <li>name</li>
                  <li>location</li>
                  <li>latitude</li>
                  <li>longitude</li>
            </ul>
      </li>
      <li><strong>time</strong> - timestamps of records in songplays broken down into specific units
            <ul>
                  <li>start_time</li>
                  <li>hour</li>
                  <li>day</li>
                  <li>week</li>
                  <li>month</li>
                  <li>year</li>
                  <li>weekday</li>
            </ul>
      </li>
</ol>

