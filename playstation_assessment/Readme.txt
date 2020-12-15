Tech Stack used: Cloudera based HDFS, Hive, Pyspark, Sql, Python

1. I have used data storage for the hive and created a data model. Data model contains 3 tables based on the input file . Please refer to the Datamodel_hive.hql file for this code snippet.

2. Pyspark is the processing engine chosen for this assessment ,  Processed input files using Pyspark and loaded into the hive tables. Please refer to the Dim_Restaurent_Profile_Details_Pysprk_load.py and Dim_User_and_Fact_user_visit_details_Pyspark_Load.py files.

3. Created a pyspark based function for the interview questions 1-5 . Please refer the playstation_assessment_pyspark_functions.py file

4. Created Pyspark SQL queries as well for the interview questions 1-5. Please refer to the playstation_assessment_sql.py file.

Please Note : Comments are given inline in each code file for reference. I have tested the results and did unit tests manually and if the team wants to do this in pytest using asserts I can do it in a couple days. Because of the timeline and my workload I did it manually.