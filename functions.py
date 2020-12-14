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

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.acl.default", "BucketOwnerFullControl")

    return spark

def get_dataframes(spark):

    place_details_df=spark.table("rubric.restaurant_profile_details")

    user_details_df=spark.table("rubric.user_profile_details")

    user_visit_details_df=spark.table("rubric.user_visit_details")

    return (place_details_df,user_details_df,user_visit_details_df)
    
def top_restaurants_by_salesamount(spark,place_details_df,user_visit_details_df,startdate,enddate):

#Using this function to get the Total sales amount and distinct servedCuisines based #on place id and ranking them in descnding order. This function is reused to determine #both top N and top Nth resturents by the salesamount.

    window_spec=Window.partitionBy("servedcuisines").orderBy(col("tot_sales_amt").desc())
    place_details_df=place_details_df.select(col("placeid"),explode(col("servedCuisines")).alias("servedCuisines"))
    
    distinct_cuisines=place_details_df.select(col("servedCuisines")).distinct()
    
    user_visit_details_df=user_visit_details_df.filter(col("visitdt").between(lit(startdate),lit(enddate))) \
                            .select(col("placeid"),col("salesAmount"))
                            
    join_df=place_details_df.join(user_visit_details_df,["placeid"],"inner").groupBy("placeid","servedcuisines") \
                            .agg(sum(col("salesamount")).alias("tot_sales_amt")) \
                            .withColumn("rn",row_number().over(window_spec))

    return (join_df,distinct_cuisines)

def top_Nth_restaurant(spark,place_details_df,user_visit_details_df,startdate,enddate,N):

    (join_df,distinct_cuisines)=top_restaurants_by_salesamount(spark,place_details_df,user_visit_details_df,startdate,enddate)
    
    restaurants_by_sales_amount_df=join_df.filter(col("rn") == lit(N)) 
    
    topN_df=distinct_cuisines.join(restaurants_by_sales_amount_df,["servedCuisines"],"left_outer").select(col("servedcuisines"),col("placeid"))

    return topN_df

def top_N_restaurants(spark,place_details_df,user_visit_details_df,startdate,enddate,N):

    (join_df,distinct_cuisines)=top_restaurants_by_salesamount(spark,place_details_df,user_visit_details_df,startdate,enddate)
    
    restaurants_by_sales_amount_df=join_df.filter(col("rn") <= lit(N))
                            
    top3_df=distinct_cuisines.join(restaurants_by_sales_amount_df,["servedCuisines"],"left_outer").select(col("servedcuisines"),col("placeid"))

    return top3_df



def avg_hours_consective_visits(spark,user_visit_details_df,visitdate):
# First I am fetching the user records for the visit dtae and using lag function get the precious visit date for the user
#calculating the time difference in seconds. Filtering outs the records if the previous visit time is zero and rank is not 1.
#Converting the time difference from sconds to hours and if there is no previous visit then I am defaulting the value to -9994.

    window_spec=Window.partitionBy("userid").orderBy(col("visitdate"))
    window_spec1=Window.partitionBy("userid").orderBy(col("visitdate").desc())
    avg_hours_df=user_visit_details_df \
                .filter(col("visitdt") == lit(visitdate)) \
                .selectExpr("userid","cast(cast(visitdate as timestamp) as long)  as visitdate") \
                .withColumn("prev_visitdate",lag(col("visitdate"),1,0).over(window_spec)) \
                .withColumn("time_diff_in_secs", (col("visitdate") - col("prev_visitdate"))) \
                .withColumn("rn",row_number().over(window_spec1)) \
                .filter(~((col("prev_visitdate") == lit(0)) & (col("rn") != lit(1)))) \
                .withColumn("diff_in_hours",when(col("prev_visitdate") == lit(0),lit(-9994)) \
                .otherwise(round(col("time_diff_in_secs")/3600,2))) \
                .groupBy("userid").agg(round(avg(col("diff_in_hours")),2).alias("avg_time"))
                
    return avg_hours_df

def top_N_restaurants_by_rating(spark,user_details_df,place_details_df,user_visit_details_df,userid,N):
#First we are filtering the data to fecth the user favorite restaurent.Once we have user favrite cusine we are using this data to filter them from place details
#Aggregating the dataset based on Place id to calculate the average of restaurant ratings and ranking them accordingly to fetch top n restaurant.
    
    window_spec=Window.orderBy(col("avg_rating").desc())

    user_cuisine=user_details_df.filter(col("userid") == lit(userid)).select(explode(col("favcuisine")).alias("favcuisine")).limit(1)
    
    place_details_df=place_details_df.select(col("placeid"),explode(col("servedCuisines")).alias("servedCuisines"))
    
    if user_cuisine.count() == 1:
        user_cuisine=user_cuisine.collect()[0][0]
        place_details_df=place_details_df.filter(col("servedCuisines")==lit(user_cuisine))

    user_visit_details_df=user_visit_details_df.select(col("placeid"),col("restrating"))

    join_df=place_details_df.join(user_visit_details_df,["placeid"],"inner").groupBy("placeid") \
                        .agg(round(avg(col("restrating")),2).alias("avg_rating")) \
                        .withColumn("rn",row_number().over(window_spec))  \
                        .filter(col("rn") <= lit(N)) \
                        .drop("rn")

    return join_df

def top_N_restaurants_by_rating_user_not_visited(spark,user_details_df,place_details_df,user_visit_details_df,userid,N):
#Here I took the same approach as my previous function
#First I am fetching the user favorite restuarent.In the place_details_not_user_visted_df I am fteching all the place id for that perticular user with null values
#Aggregating the dataset based on Place id to calculate the average of restaurant ratings and ranking them accordingly to fetch top n restaurant.
    
    window_spec=Window.orderBy(col("avg_rating").desc())

    user_cuisine=user_details_df.filter(col("userid") == lit(userid)).select(explode(col("favcuisine")).alias("favcuisine")).limit(1)
    
    place_details_df=place_details_df.select(col("placeid"),explode(col("servedCuisines")).alias("servedCuisines"))
    if user_cuisine.count() == 1:
        user_cuisine=user_cuisine.collect()[0][0]
        place_details_df=place_details_df.filter(col("servedCuisines")==lit(user_cuisine))
        
    user_visited_places_df=user_visit_details_df.filter(col("userid") == lit(userid)).select(col("placeid"))

    place_details_not_user_visted_df = place_details_df \
                        .join(user_visited_places_df,place_details_df.placeid == user_visited_places_df.placeid,"left_outer") \
                        .filter(user_visited_places_df["placeid"].isNull()) \
                        .select(place_details_df["placeid"])
                        

    user_visit_details_df=user_visit_details_df.select(col("placeid"),col("restrating"))

    join_df=place_details_not_user_visted_df.join(user_visit_details_df,["placeid"],"inner").groupBy("placeid") \
                        .agg(round(avg(col("restrating")),2).alias("avg_rating")) \
                        .withColumn("rn",row_number().over(window_spec))  \
                        .filter(col("rn") <= lit(N)) \
                        .drop("rn")

    return join_df

if __name__ == "__main__":

    """if(len(sys.argv)) != 2:
        raise Exception("Insufficient input arguments provided......")"""

    #Instantiate Spark Session
    spark = get_spark_session("sparkprocess")

    #gets all the files needed for analysis as dataframes
    (place_details_df,user_details_df,user_visit_details_df)=get_dataframes(spark)

    #Question 1:  What are the top three restaurants for each type of cuisine served #by sales amount for a given period? 
    
    startdate='2020-05-09'
    enddate='2020-05-10'
    N=3
    top_n_restaurents_df=top_N_restaurants_by_salesamount(spark,place_details_df,user_visit_details_df,startdate,enddate,N)
    top_n_restaurents_df.show()

    #Question 2: Develop function(s) that takes in an integer parameter (N) to get Nth top restaurant by sales amount for each type of cuisine for a specific period. 
    startdate='2020-05-09'
    enddate='2020-05-10'
    N=3
    top_nth_restaurents_df=top_Nth_restaurant_by_salesamount(spark,place_details_df,user_visit_details_df,startdate,enddate,N)
    top_nth_restaurents_df.show()

    #Question 3: Develop function(s) that takes in a date (visit date) to get average hours between two consecutive visits to any restaurants for all the users that visited any restaurant on that date. 
    visitdate="2020-05-10"
    dfave_hours_by_users_df=avg_hours_consective_visits(spark,user_visit_details_df,visitdate)
    dfave_hours_by_users_df.show()

    #Question 4: Develop function(s) that takes in a userId, to recommend Top N restaurants by rating (restRating field in placeInteractionDetails attribute in userDetails.json file) for that particular user based on his cuisine preference  
    userid="U1042"
    N=3
    top_n_fave_cuisines_dg=top_N_restaurants_by_rating(spark,user_details_df,place_details_df,user_visit_details_df,userid,N)
    top_n_fave_cuisines_dg.show()

    #Question 5: Add functionality to Question 4 to recommend only restaurants that user has not visited yet. If all those recommended restaurants have been visited by user, return empty list.  
    userid="U1042"
    N=3
    top_n_fav_cuisine_not_visited_df=top_N_restaurants_by_rating_user_not_visited(spark,user_details_df,place_details_df,user_visit_details_df,userid,N)
    top_n_fav_cuisine_not_visited_df.show()
    