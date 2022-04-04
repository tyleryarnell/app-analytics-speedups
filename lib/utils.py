import os
import logging
import boto3
import requests
from types import SimpleNamespace
from datetime import datetime , timedelta, date
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

# ETL libraries for Glue
from awsglue.context import GlueContext
from pyspark.context import SparkContext


def init_logger():
    logging.basicConfig(format='%(asctime)s %(levelname)s %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
    logger.setLevel(logging.INFO)
    
    return logger

def get_list_of_days_as_formatted_string(frequency):
    return "".join(
        ("{" ,",".join(["{:%Y-%m-%d}".format(datetime.now() - timedelta(days=i))for i in range(0 , frequency + 1)]),"}",))
    

def build_s3_paths(args):
    freq = int(args['frequency'])
    s3_src_bkt = args['s3_sourc_bucket_name']
    s3_merg_bkt = args['s3_merged_bucket_name']
    s3_out_bkt = args['s3_output_bucket_name']

    app_track_days = get_list_of_days_as_formatted_string(freq)
        
    s3_paths = {
        'apps': f's3://{s3_src_bkt}/apps/',
        'cust_apps': f"s3://{s3_src_bkt}/customer_apps/customer_id=*/*.json",
        'app_tracking': f's3://{s3_merg_bkt}/dt={app_track_days}',
        'users': f's3://{s3_merg_bkt}/relay_users_production/',
        'session_count_by_group_id': f's3://{s3_out_bkt}/frequency={freq}/ssnCNTgrpgid/'
        }
    
    return SimpleNamespace(**s3_paths)

    
def get_times(args):
    freq = int(args['frequency'])
    # Start and end date of the frequency windowframe
    start_date = date.today()-timedelta(days=1)
    end_date = date.today() - timedelta(days=freq)
    start_date_lst = end_date - timedelta(days=1)
    end_date_lst = end_date - timedelta(days=freq)

    # Timestamp to filter out all at_hour days not inluded in the desired range
    start_ts = datetime.combine(date.today()-timedelta(days=1), datetime.max.time())
    end_ts = datetime.combine(date.today()-timedelta(days=freq), datetime.min.time())

    # Timestamp to filter out all at_hour days not inluded in the desired range, but for prior period with same frequency 
    start_ts_lst = datetime.combine(start_date_lst, datetime.max.time())
    end_ts_lst = datetime.combine(end_date_lst, datetime.min.time())
    
    times = {
        'start_date': start_date,
        'end_date': end_date,
        'start_date_lst': start_date_lst,
        'end_date_lst': end_date_lst,
        'start_ts': start_ts,
        'end_ts': end_ts,
        'start_ts_lst': start_ts_lst,
        'end_ts_lst': end_ts_lst,
    }
    
    return SimpleNamespace(**times)


def init_etl_job(args):

    freq = int(args['frequency'])
    secret_name = args['secret_name']
    
    times = get_times(freq)
    s3_paths = build_s3_paths(args)
    
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager" , region="us-west-2")
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    os.environ['API_KEY'] = eval(get_secret_value_response[ "SecretString" ])[ "apiKEY" ]
    os.environ['ENDPOINT'] = eval(get_secret_value_response[ "SecretString" ])[ "url" ]

    glueContext = GlueContext(SparkContext.getOrCreate())
    glueContext._jsc.hadoopConfiguration().set("fs.s3.canned.acl" , "BucketOwnerFullControl")

    spark = glueContext.spark_session
    spark.conf.set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" , 2)
    spark.conf.set("spark.sql.broadcastTimeout", 600)
    return client, spark, times, s3_paths

def filter_app_tracking_data(df_app_track, times):
    return df_app_track.where(F.col("at_hour").between(times.end_ts, times.start_ts))


@F.udf(returnType=IntegerType())
def retOffset(custId):
    headers = {
        "x-api-key" : os.environ['API_KEY'] ,
        "customerId" : custId ,
        "Content-Type" : "application/json" ,
    }
    try :
        return int(requests.get(os.environ['ENDPOINT'] , headers=headers).json()[ "utc_offset" ])
    except :
        return 0
 

def load_customer_apps_data(spark, s3_cust_apps_path):
    return spark.read.format("json").load(s3_cust_apps_path)


def filter_customer_apps_data(df_cust_apps):
    df_cust_apps = (df_cust_apps.withColumn("customer_id" ,
                                F.regexp_extract(F.input_file_name(),"\/customer_id=([^\/]*)\/",1),)
        .filter("is_tracked=='true'")
        .withColumn("offset", retOffset(F.col("customer_id"))
                   .alias("offset"))
        )
    return df_cust_apps

    
def load_apps_data(spark, s3PrdRd):
        return spark.read.format("json").load(f"s3://{s3PrdRd}/apps/")
    

def load_user_data(spark, s3_users_path):    
    return spark.read.parquet(s3_users_path)


def filter_user_data(df_users, distinct_customer_vals):
    df_users = (
        df_users.select(
            F.col("Item")[ "customer_id" ][ "S" ].alias("customer_id") ,
            F.col("Item")[ "email" ][ "S" ].alias("email") ,
            F.col("Item")[ "user_type" ][ "N" ].alias("user_type") ,
            F.col("Item")[ "guid" ][ "S" ].alias("user_guid") ,
            F.col("Item")[ "first_name" ][ "S" ].alias("fname") ,
            F.col("Item")[ "last_name" ][ "S" ].alias("lname") ,
            F.col("Item")[ "parent_group_guids" ][ "SS" ].alias("parent_group_guids") ,
            )
        .filter(F.col('customer_id').isin(distinct_customer_vals))
        )
    
    return df_users

    
def get_list_of_last_period_days_as_f_string(frequency):
    return "".join(
        ("{" , ",".join([ "{:%Y-%m-%d}".format(datetime.now() - timedelta(days=i)) for i in range(frequency , frequency + frequency) ]) ,
         "}"))


def load_app_tracking_data(spark, s3_app_tracking_path):
    # Load app tracking data for the given list of days
    return spark.read.load(s3_app_tracking_path)


def load_prior_period_app_tracking_data(spark, app_tracking_path, freq):
    apTrckdysLst = get_list_of_last_period_days_as_f_string(freq)
    return spark.read.load(f"s3://{app_tracking_path}/dt={apTrckdysLst}")