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


if __name__ == "__main__":

    """if(len(sys.argv)) != 2:
        raise Exception("Insufficient input arguments provided......")"""

    #Instantiate Spark Session
    spark = get_spark_session("sparkprocess")


    #gets all the data frames for analysis
    (place_details_df,user_details_df,user_visit_details_df)=get_dataframes(spark)

    place_details_df.createOrReplaceTempView("place_details")

    user_details_df.createOrReplaceTempView("user_details")

    user_visit_details_df.createOrReplaceTempView("user_visit_details")

    #Question 1: Top N restaurants for each cuisines by sales amount for a given period
    N=3
	x='2020-05-09'
	y='2020-05-10'

	query1= f"""select * from 
	(select placeid,servedcuisines,tot_amount,dense_rank() over(partition by servedcuisines order by tot_amount desc) as rk from
	(select placeid,servedcuisines,sum(salesamount) as tot_amount from
	(select a.placeId,explode(a.servedCuisines) as servedcuisines ,b.salesAmount 
	from place_details a inner join user_visit_details b on a.placeid = b.placeid where substring(b.visitdate,1,10) between '{x}' and '{y}')c
	group by placeid,servedcuisines)d)e
	where rk<={N}
	"""
	top_n_restaurents_df=spark.sql(query1)
	top_n_restaurents_df.show()
    

    #Question 2:  top Nth restaurant for each cuisines by sales amount for a given period
    N=10
	x='2020-05-09'
	y='2020-05-10'
	query2= f"""select * from 
	(select placeid,servedcuisines,tot_amount,dense_rank() over(partition by servedcuisines order by tot_amount desc) as rk from
	(select placeid,servedcuisines,sum(salesamount) as tot_amount from
	(select a.placeId,explode(a.servedCuisines) as servedcuisines ,b.salesAmount 
	from place_details a inner join user_visit_details b on a.placeid = b.placeid where substring(b.visitdate,1,10) between '{x}' and '{y}')c
	group by placeid,servedcuisines)d)e
	where rk={N}
	"""
	top_nth_restaurents_df=spark.sql(query2)
	top_nth_restaurents_df.show()





    #Question 3:  Get average hours between two consecutive visits to any restaurants for all the users that visited on any particular date
     
     x='2020-05-10'
	query3=f"""select userid,coalesce(round(avg(cast(diff_in_secs as float)/3600),2),-9994) as avg_diff_in_hours from
	(select *,(cast(cast(visitdate as timestamp) as long) - cast(cast(prev_vistidate as timestamp) as long)) 
	as diff_in_secs ,row_number() over(partition by userid order by visitdate desc) as rn from
	(select *,lag(visitDate,1,0) over(partition by userID order by visitdate) as prev_vistidate
	from user_visit_details where substring(visitdate,1,10) = '{x}')a)b 
	where not (rn<>1  and diff_in_secs is null) group by userid"""
	print(query3)
	ave_hours_by_users_df=spark.sql(query3)
	dfave_hours_by_users_df.show(10)


    #Question 4: Top N restaurants by rating for user favorite cuisine including user visited places 

    N=10
	given="U1042"
	query4=f"""select b.placeid,round(avg(c.restrating),2) as avg_rating from
	(select explode(favcuisine) as favcuisine from user_details where userid = '{given}' limit 1)a inner join
	(select placeid ,explode(servedCuisines) as servedCuisines from place_details) b on a.favcuisine = b.servedCuisines
	inner join user_visit_details c  on b.placeid = c.placeid
	group by b.placeid
	order by avg_rating desc
	limit {N}
	"""
	top_n_fave_cuisines_df=spark.sql(query4)
	top_n_fave_cuisines_df.show()
    

    #Question 5: Top N restaurants by rating for user favorite cuisine excluding user visited places 
    N=10
	given="U1042"
	query5=f"""select b.placeid,round(avg(c.restrating),2) as avg_rating from
	(select explode(favcuisine) as favcuisine from user_details where userid = '{given}' limit 1)a inner join
	(select placeid ,explode(servedCuisines) as servedCuisines from place_details) b on a.favcuisine = b.servedCuisines
	inner join user_visit_details c  on b.placeid = c.placeid
	left outer join (select placeid from user_visit_details where userid = '{given}')d on b.placeid = d.placeid
	where d.placeid is null
	group by b.placeid
	order by avg_rating desc
	limit {N}
	"""
	top_n_fav_cuisine_not_visited_df=spark.sql(query5)
	top_n_fav_cuisine_not_visited_df.show()


