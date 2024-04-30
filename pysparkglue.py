### Iport Libraries and set variables

#Import Python modules
from datetime import datetime

#Import pyspark modules
from pyspark.context import SparkContext
Import pypspark.sql.functions as f

#Import glue module
from awsglue.utils import getResolevedOptions
from awsglue.context import GLueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import job

#Initializing contexts and sessions
spark_context = SparkContext.GetOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

#Parametrs
glue_dv = "virtual"
glue_tbl = "import"
s3_write_path = "s3://youtubeliveglue/output"



### Extract (Read Data)


dynamic_frame_read = glue_context.create_dynamic_frame.from_catalog(database = glue_db, table_name = glue_tbl)

#Convert dynamic fram to data to use standard pyspark functions
data_frame = dynamic_frame_read.toDF()


### Transform (Modify Data)


data_frame_aggregated = data_frame.groupby("VendorID").agg(
    f.mean(f.col("total_amount")).alias('avg_total_amount'),
    f.mean(f.col("trip_time")).alias('avg_trip_time'),
)

data_frame_aggregated = data_frame_aggregated.orderBy(f.desc("avg_total_amount"))



### Load (write Data)

#Create just 1 partition, because there is so little data
data_frame_aggregated = data_frame_aggregated.reoartition(1)

#Convert back tp dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(data_frame_aggregated, glue_context, "dynamic_frame_write")4

#Write data back to S3
glue.context.write_dynamic_frame.from_options(
    frame = dynamic_frame_write,
    connection_type = "s3",
    connection_options = {
        "path": s3_write_path,
    },

    format ="csv"
)