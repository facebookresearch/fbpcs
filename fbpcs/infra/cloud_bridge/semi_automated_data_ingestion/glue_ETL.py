# Copyright (c) Facebook, Inc. and its affiliates.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


#########################################
### IMPORT LIBRARIES AND SET VARIABLES
#########################################

#Import python modules
from datetime import datetime
import sys

#Import pyspark modules
from pyspark.context import SparkContext
from pyspark.sql.functions import col,year,month,dayofmonth,hour,to_date,lit,from_unixtime
from pyspark.sql.types import IntegerType

#Import glue modules
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame


#Initialize contexts and session
spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
session = glue_context.spark_session

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           's3_read_path',
                           's3_write_path'])

#Parameters

s3_options = {"paths": ["s3://" + args['s3_read_path']]}
s3_write_path = "s3://" + args['s3_write_path']
#########################################
### EXTRACT (READ DATA)
#########################################

#Log starting time
dt_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("Start time:", dt_start)

# read data from s3 directly
dynamic_frame_read = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options=s3_options,
    format="csv",
    format_options={"withHeader": True},
)  # format_options go by default

# #Convert dynamic frame to data frame to use standard pyspark functions
data_frame = dynamic_frame_read.toDF()

### debug
data_frame.show(10)

#########################################
### TRANSFORM (MODIFY DATA)
#########################################

# create columns
augmented_df = (
    data_frame.withColumn("unixtime", data_frame["timestamp"].cast(IntegerType()))
    .withColumn("action_source", lit("app"))
    .withColumn("date_col", to_date(from_unixtime(col("unixtime"))))
    .withColumn("year", year(col("date_col")))
    .withColumn("month", month(col("date_col")))
    .withColumn("day", dayofmonth(col("date_col")))
    .withColumn("hour", hour(col("date_col")))
    .drop(col("date_col"))
    .drop(col("unixtime"))
    .repartition(1)
)

#Print result table
#Note: Show function is an action. Actions force the execution of the data frame plan.
#With big data the slowdown would be significant without cacching.
final_df = augmented_df
final_df.show(10)

#########################################
### LOAD (WRITE DATA)
#########################################

#Create just 1 partition, because there is so little data
final_df = final_df.repartition(1)

#Convert back to dynamic frame
dynamic_frame_write = DynamicFrame.fromDF(final_df, glue_context, "dynamic_frame_write")

#Write data back to S3
glue_context.write_dynamic_frame.from_options(
frame = dynamic_frame_write,
connection_type = "s3",
connection_options = {
"path": s3_write_path,
#Here you could create S3 prefixes according to a values in specified columns
"partitionKeys": ["year", "month", "day", "hour"]
},
format = "json"
)

#Log end time
dt_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print("End time:", dt_end)
