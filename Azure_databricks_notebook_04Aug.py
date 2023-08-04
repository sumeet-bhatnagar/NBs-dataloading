# Databricks notebook source
dbutils.fs.put('/tmp/abc.txt','Hello World from DBFS',True)

# COMMAND ----------



# COMMAND ----------

# dbutils.fs.ls('s3://bkt-databricks-sumeetb-aug4/tmp/iris.csv')

df = spark.read.csv('s3://bkt-databricks-sumeetb-aug4/tmp/iris.csv')
# df.head()

access_key = ""

secret_key = ""



#Mount bucket on databricks

encoded_secret_key = secret_key.replace("/", "%2F")

aws_bucket_name = "bkt-databricks-sumeetb-aug4"

mount_name = "sumeetbawss3"

# dbutils.fs.mount("s3a://%s:%s@%s" % (access_key, encoded_secret_key, aws_bucket_name), "/mnt/%s" % mount_name)

# display(dbutils.fs.ls("/mnt/%s" % mount_name))

df.write.parquet(f'/mnt/{mount_name}/tmp/')


# mount_name = "nithinawss3"

# file_name="iris.csv"

# df = spark.read.format("csv").load("/mnt/%s/%s" % (mount_name , file_name))

# df.show()

#spark.read.format("csv").load("s3://bkt-nithin-04aug/iris.csv")

# COMMAND ----------

df = spark.sql('select * from CSV.`s3://bkt-databricks-sumeetb-aug4/tmp/iris.csv`')

df.head(10)

# COMMAND ----------


