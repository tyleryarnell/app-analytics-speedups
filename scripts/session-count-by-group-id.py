# Python libraries
import sys
import os
import logging

# ETL libraries for Glue
from awsglue.utils import getResolvedOptions

# Custom libraries
from lib.utils import init_logger, init_etl_job, load_app_tracking_data, filter_app_tracking_data, load_customer_apps_data, filter_customer_apps_data, load_user_data, filter_user_data
from lib.transforms import transform_session_count_data, agg_session_data_by_group_count

logger = init_logger()

# ************ SETUP ENVIRONMENT VARIABLE // GET SECRETS // INSTANTIATE CLIENTS ************ #
expected_args = [
    "secret_name", 
    "s3_source_bucket_name", 
    "s3_merged_bucket_name", 
    "s3_output_bucket_name",
    "frequency" ,
    "env"]
args = getResolvedOptions(sys.argv , expected_args)

logger.info({
    "frequency": args['frequency'],
    "env": args['env'],
})

_, spark, times, s3_paths = init_etl_job(args)

# ************ ETL JOB STARTS HERE ************ #
logger.info('Loading app tracking data...')
df_app_track = load_app_tracking_data(spark, s3_paths.app_tracking_path)
df_app_track = filter_app_tracking_data(df_app_track, times)

# Read the customer apps data from the customer apps table
logger.info('loading customer apps data.')
df_cust_apps = load_customer_apps_data(spark, s3_paths.cust_apps)
df_cust_apps = filter_customer_apps_data(df_cust_apps)

# Read user data from the Relay users table
df_users = load_user_data(spark, s3_paths.users)
dist_customers = df_cust_apps.select("customer_id").distinct().collect()
dist_customers = [ v[ "customer_id" ] for v in dist_customers ]
df_users = filter_user_data(df_users, dist_customers)
df_users.cache().count()

# Get the group sessions count and after transforms
df_transformed = transform_session_count_data(df_cust_apps, df_users, df_app_track)
df_transformed.cache().count()

# Aggregation of session count per group, per date, per app, per customer in last n days
df_agg = agg_session_data_by_group_count(df_transformed)

# Save session count data
df_agg.write.mode("overwrite").partitionBy("customer_id" , "app_guid").save(s3_paths.session_count_by_group_id)