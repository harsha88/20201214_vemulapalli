#This code is for laoding the data into Fact_user_visit_details,Dim_user_profile_details table from userdetails.json file.

#userdetails file data is modeled into 2 different data set , And user visit transaction details are stored in Fact_user_visit_details table by the visit_date partition.

#Another table Dim_user_profile_details is dimension contains the user attributes and loading in overwrite based on the latest data .

# Added indicator 1 to data from the file and indicator 2 for the data in table. Used row_number function to determine which one is latest based on indicator. Combined both data frames from file and dimensional table and filtered where row_number = 1 to load updated place details in Dim_user_profile_details table.

#This data is laoded into the table in overwrite mode which will delete the old dataset and load the new dataset

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.Row
import sys
from config import *
from pyspark.sql.window import Window

s

def get_spark_session(appName):

    spark = SparkSession \
        .builder \
        .appName(appName) \
        .enableHiveSupport \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark

def updateUserDetails(spark,inputpath):

    userDetails_df = spark.read.json(inputpath)

    if userDetails_df.limit(1).count() > 0:
        placeInteractionDetails_df=user_details_df.select(col("userID"),explode(col("placeInteractionDetails")).alias("tmp"))
       
        userVisitDetails_df=placeInteractionDetails_df.select(col("userId"),col("tmp.placeID"),col("tmp.restRating"),col("tmp.foodRating"),col("tmp.serviceRating"),col("tmp.salesAmount"),col("tmp.visitDate"))

        userVisitDetails_df.withColumn("visitdt",substrint(col("visitdate"),1,10)).write.mode(SaveMode.Append).partitionBy("visitdt").insertInto("rubric.Fact_user_visit_details") \
        .withColumn("dw_created_ts",current_timestamp())

        incUserDetails_df=userDetails_df.drop('placeInteractionDetails').withColumn("ind",lit(1))\
        .withColumn("dw_created_ts",current_timestamp())

        fullUserDetails_df=spark.table("rubric.Dim_user_profile_details").withColumn("ind",lit(2))

        if fullUserDetails_df.limit(1).count() > 0:
            window1 = Window.partitionBy("placeID").orderBy("ind")

            latestUserProfileDetails_df=fullUserDetails_df.union(incUserDetails_df).withColumn("rn",row_number().over(window1)).filter(col("rn") == lit(1)).drop("ind","rn")
        else:
            latestUserProfileDetails_df=incUserDetails_df.drop("rn")
        latestUserProfileDetails_df.write.mode(SaveMode.Overwrite).insertInto("rubric.Dim_user_profile_details")

   
if __name__ == "__main__":

    if(len(sys.argv)) != 2:
        raise Exception("Insufficient input arguments provided......")

    #Instantiate Spark Session
    spark = get_spark_session("sparkprocess")

    input_path = args[1]
   
    df=updateUserDetails(spark,input_path)
