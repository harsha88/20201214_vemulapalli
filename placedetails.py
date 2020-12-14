# This code is for loading the data into restaurant_profile_details table from the placedeatils file.
#Here we are inserting and updating the table with latest data in overwrite mode based on placeID.
# Added indicator 1 to data from the file and indicator 2 for the data in table. Used row_number function to determine which one is latest based on indicator. Combined both data frames from file and dimensional table and filtered where row_number = 1 to load updated place details in restaurent_profile_details table.

#This data is loaded into the table in overwrite mode which will overwrite the existing data based on the latest data from file , if there is no updates to existing placeid in the file then it will stay same in the table.

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.Row
import sys
from config import *
from pyspark.sql.window import Window

def get_spark_session(appName):

    spark = SparkSession \
        .builder \
        .appName(appName) \
        .enableHiveSupport \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    return spark

def updatePlaceDetails(spark,inputpath):

    placeDetails_df = spark.read.json(inputpath)

    if placeDetails_df.limit(1).count() > 0:
        incPlaceDetails_df=placeDetails_df.withColumn("ind",lit(1))
        \.withColumn("dw_created_ts",current_timestamp())

        fullPlaceDetails_df=spark.table("rubric.restaurant_profile_details").withColumn("ind",lit(2))

        if fullPlaceDetails_df.limit(1).count() > 0:
            window1 = Window.partitionBy("placeID").orderBy("ind")

            latestPlaceProfileDetails_df=fullPlaceDetails_df.union(incPlaceDetails_df).withColumn("rn",row_number().over(window1)).filter(col("rn") == lit(1)).drop("ind","rn")
        else:
            latestPlaceProfileDetails_df=incPlaceDetails_df.drop("rn")
        latestPlaceProfileDetails_df.write.mode(SaveMode.Overwrite).insertInto("rubric.restaurant_profile_details")

   
if __name__ == "__main__":

    if(len(sys.argv)) != 2:
        raise Exception("Insufficient input arguments provided......")

    #Instantiate Spark Session
    spark = get_spark_session("sparkprocess")

    input_path = args[1]
   
    df=updatePlaceDetails(spark,input_path)
