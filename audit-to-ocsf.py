import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import unix_timestamp, col, lit, date_format

args = getResolvedOptions(sys.argv, ["JOB_NAME", "path"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3_path = args["path"]

# Read the CSV file from Amazon S3
AmazonS3_node1692708345833 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [s3_path]},
    transformation_ctx="AmazonS3_node1692708345833",
)

# Convert to DataFrame
df = AmazonS3_node1692708345833.toDF()

transformed_df = (
    df.withColumn("time", (unix_timestamp("timestamp") * 1000).cast("long"))
    .withColumn("log_level", col("log_level"))
    .withColumn("service_name", col("service_name"))
    .withColumn("message", col("message"))
    .withColumn("user_id", col("user_id"))
    .withColumn("session_id", col("session_id"))
    .withColumn("client_ip_address", col("client_ip_address"))
    .withColumn("activity_id", lit(0).cast("int"))
    .withColumn("category_uid", lit(2).cast("int"))
    .withColumn("class_uid", lit('2001'))
    .withColumn("title", lit(''))
    .withColumn("uid", lit(''))
    .withColumn("product_name", lit('mixed_audit_logs'))
    .withColumn("vendor_name", lit('Amazon'))
    .withColumn("version", lit('1.0.0-rc.3'))
    .withColumn("severity_id", lit(0).cast("int"))
    .withColumn("state_id", lit(0).cast("int"))
    .withColumn("type_uid", lit('200100'))
    .withColumn("region", lit('us-east-1'))
    .withColumn("accountId", lit('926795357979'))
    .withColumn("eventDay", date_format("timestamp", "yyyyMMdd"))
)

# Convert back to DynamicFrame
ChangeSchema_node1692708742183 = DynamicFrame.fromDF(transformed_df, glueContext, "ChangeSchema_node1692708742183")

ChangeSchema_node1692708742183 = ApplyMapping.apply(
    frame=ChangeSchema_node1692708742183,
    mappings=[
        ("activity_id", "int", "activity_id", "int"),
        ("category_uid", "int", "category_uid", "int"),
        ("class_uid", "string", "class_uid", "int"),
        ("time", "long", "time", "long"),
        ("title", "string", "finding.title", "string"),
        ("log_level", "string", "finding.supporting_data.log_level", "string"),
        ("service_name", "string", "finding.supporting_data.service_name", "string"),
        ("user_id", "string", "finding.supporting_data.user_id", "int"),
        ("session_id", "string", "finding.supporting_data.session_id", "string"),
        (
            "client_ip_address",
            "string",
            "finding.supporting_data.client_ip_address",
            "string",
        ),
        ("uid", "string", "finding.uid", "string"),
        ("message", "string", "message", "string"),
        ("product_name", "string", "metadata.product.name", "string"),
        ("vendor_name", "string", "metadata.product.vendor_name", "string"),
        ("version", "string", "metadata.version", "string"),
        ("severity_id", "int", "severity_id", "int"),
        ("state_id", "int", "state_id", "int"),
        ("type_uid", "string", "type_uid", "int"),
        ("region", "string", "region", "string"),
        ("accountId", "string", "accountId", "string"),
        ("eventDay", "string", "eventDay", "string"),
    ],
    transformation_ctx="ChangeSchema_node1692708742183",
)

# Write to Amazon S3
AmazonS3_node1692709006796 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1692708742183,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://aws-security-data-lake-us-east-1-agtsuagwlvaqdc8bjdpzgh8nuyzugm/ext/Application_Audit_Log/",
        "partitionKeys": ["region", "accountId", "eventDay"],
    },
    format_options={"compression": "gzip"},
    transformation_ctx="AmazonS3_node1692709006796",
)

job.commit()
