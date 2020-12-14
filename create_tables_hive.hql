#I am going with Star Schema approach based on the input files.
# Place details file has restaurent details and created this as dimension table 
# I have divided user details into 2 tables, user_profile_details and user_visit_details . 
# User_profile_details is dimension contains user profile related attributes
# user_visit_details is fact table and storing measure related to user visits.

#Create new database for this project called Rubric 
# We can have more table properties while creating tables based on project or requirement needs. I have just added properties based on the test environment in local.
 
create database if not exists rubric;

drop table if exists rubric.restaurant_profile_details;

create table if not exists rubric.restaurant_profile_details
(
    placeId             string,
    servedCuisines      array<string>,
    acceptedPayments    array<string>,
    openHours           array<struct<hours string,days string>>,
    parkingType         array<string>,
	dw_created_ts       timestamp   
)
stored as parquet 
location '/user/cloudera/hive/tables/restaurant_profile_details'
tblproperties('parquet.compression'='SNAPPY');


drop table if exists rubric.user_profile_details;

create table if not exists rubric.user_profile_details
(
    userID string,
    latitude string,
    longitude string,
    smoker              string,
    drink_level string,
    dress_preference string,
    ambience string,
    transport string,
    marital_status string,
    hijos string,
    birth_year string,
    interest string,
    personality string,
    religion string,
    activity string,
    weight string,
    budget string,
    height string,
    userPaymentMethods array<string>,
    favCuisine array<string>,
	dw_created_ts       timestamp  
)
stored as parquet 
location '/user/cloudera/hive/tables/user_profile_details'
tblproperties('parquet.compression'='SNAPPY');

drop table if exists rubric.user_visit_details;

create table if not exists rubric.user_visit_details
(
    userID string,
    placeID string,
    restRating string,
    foodRating string,
    serviceRating string,
    salesAmount float,
    visitdate string,
	dw_created_ts timestamp  
)
partitioned by (visitdt string)
stored as parquet 
location '/user/cloudera/hive/tables/user_visit_details'
tblproperties('parquet.compression'='SNAPPY');

