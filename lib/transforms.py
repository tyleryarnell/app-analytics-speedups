import pyspark.sql.functions as F

def transform_session_count_data(df_cust_apps, df_users, df_app_track):
    # Inner join the data with the customer apps filtered data.
    df_transformed = df_app_track.join(df_cust_apps , [ "customer_id" , "app_guid" ] , how="inner").select(
        df_app_track[ "*" ] , "offset"
    )

    # Get the count of users by hour.
    df_transformed = (
        df_transformed.withColumn(
            "date" ,
            F.to_date(
                (F.unix_timestamp("at_hour") + F.col("offset") * 60 * 60).cast(
                    "timestamp"
                )
            ) ,
        )
            .groupBy("app_guid" , "date" , "customer_id" , "email")
            .agg(F.countDistinct("at_hour").alias("snCnt"))
    )

    # Get the group sessions count, and cache the results
    return df_transformed.join(df_users , [ "email" , "customer_id" ] , how="inner").select(
        df_transformed[ "*" ] , df_users[ "parent_group_guids" ]
    )


def agg_session_data_by_group_count(df_transformed):
    df_agg = (df_transformed.withColumn("guid" , F.explode("parent_group_guids"))
        .groupBy("customer_id" , "app_guid" , "date" , "guid")
        .agg(F.sum("snCnt").alias("count"))
        .repartition("customer_id" , "app_guid"))
    return df_agg


def transform_session_data_by_user(df):
    return (df.withColumn("guid" , F.explode("parent_group_guids"))
            .groupBy("customer_id" , "app_guid" , "email" , "guid")
            .agg(F.sum("snCnt").alias("count")))

def transform_session_data_by_customer_id(df):
    return (df.groupBy("customer_id" , "app_guid" , "date")
        .agg(F.sum("snCnt").alias("count"))
        .repartition(10))
    
def transform_engagement_data_by_group_id(aptrc_Cnt, custappsFlt, useRlyFlt, aptrc_Lst, apps):
    
    apml = aptrc_Cnt.join(custappsFlt , [ "customer_id" , "app_guid" ] , how="inner").select(aptrc_Cnt[ "*" ] ,
                                                                                             "offset")
    ursnCnt = apml.withColumn("date" , F.to_date(
        (F.unix_timestamp("at_hour") + F.col("offset") * 60 * 60).cast('timestamp'))).groupBy("app_guid" , "date" ,
                                                                                              "customer_id" ,
                                                                                              "email").agg(
        F.countDistinct("at_hour").alias("snCnt"))
    grpcnt = ursnCnt.join(useRlyFlt , [ "email" , "customer_id" ] , how="inner").select(ursnCnt[ "*" ] ,
                                                                                        useRlyFlt[ "parent_group_guids" ])
    cbcnt = grpcnt.withColumn("guid" , F.explode("parent_group_guids")).groupBy("customer_id" , "app_guid" , "guid").agg(
        F.sum("snCnt").alias("NWcount") , F.countDistinct("email").alias("NWuser")) \
        .withColumn("NWengm" , (F.col("NWcount") / F.col("NWuser")).cast("int"))
    cbcntcust = grpcnt.withColumn("guid" , F.explode("parent_group_guids")).groupBy("customer_id" , "app_guid").agg(
        F.sum("snCnt").alias("NWcount") , F.countDistinct("email").alias("NWuser")) \
        .withColumn("NWengm" , (F.col("NWcount") / F.col("NWuser")).cast("int")).withColumn("guid" , F.lit("customer"))
    cntDF = cbcnt.unionByName(cbcntcust)

    apmlst = aptrc_Lst.join(custappsFlt , [ "customer_id" , "app_guid" ] , how="inner").select(aptrc_Lst[ "*" ] ,
                                                                                               "offset")
    ursnLst = apmlst.withColumn("date" , F.to_date(
        (F.unix_timestamp("at_hour") + F.col("offset") * 60 * 60).cast('timestamp'))).groupBy("app_guid" , "date" ,
                                                                                              "customer_id" ,
                                                                                              "email").agg(
        F.countDistinct("at_hour").alias("snCnt"))
    grplst = ursnLst.join(useRlyFlt , [ "email" , "customer_id" ] , how="inner").select(ursnLst[ "*" ] ,
                                                                                        useRlyFlt[ "parent_group_guids" ])
    cblst = grplst.withColumn("guid" , F.explode("parent_group_guids")).groupBy("customer_id" , "app_guid" , "guid").agg(
        F.sum("snCnt").alias("ODcount") , F.countDistinct("email").alias("ODuser")) \
        .withColumn("OLengm" , (F.col("ODcount") / F.col("ODuser")).cast("int"))
    cblstcust = grplst.withColumn("guid" , F.explode("parent_group_guids")).groupBy("customer_id" , "app_guid").agg(
        F.sum("snCnt").alias("ODcount") , F.countDistinct("email").alias("ODuser")) \
        .withColumn("OLengm" , (F.col("ODcount") / F.col("ODuser")).cast("int")).withColumn("guid" , F.lit("customer"))
    lstDF = cblst.unionByName(cblstcust)

    retDF = cntDF.join(lstDF , [ "customer_id" , "app_guid" , "guid" ] , how="left").na.fill(0)

    dfRTS = custappsFlt.selectExpr("license_count" , "annual_cost" , "round(cost_per_license,2) as cost_per_license" ,
                                   "license_type" , "cost_type" , "app_guid" , "customer_id").join(retDF ,
                                                                                                   [ "customer_id" ,
                                                                                                     "app_guid" ] ,
                                                                                                   how="left")

    return dfRTS.join(apps.selectExpr("guid as app_guid" , "name" , "developer_website" , "icon_url") , [ "app_guid" ] ,
               how="left").fillna(0) \
