#IMPORTING DEPENDENCIES
import sys
from awsglue.transforms import *
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datetime import date,timedelta

print("\nSTART\n")

#SETTING UP THE DATE                                                                         # Get date
year = str(mydate.year)                                                                         # Get Year
month = str(mydate.month) if mydate.month > 9 else '0'+str(mydate.month)                        # Get Month
day = str(mydate.day) if mydate.day > 9 else '0'+str(mydate.day)                                # Get Day
dt = year + '-' + month + '-' + day

#LOADING RUNTIME ARGS
args = getResolvedOptions(sys.argv, ['JOB_NAME','outputPath'])

#CREATING CONTEXT AND SESSION
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#LOADING DATA FROM RAW LAYER
datasource = glueContext.create_dynamic_frame.from_catalog(database = "database", 
                                                            table_name = "data_table", 
                                                            transformation_ctx = "datasource")

mapping_col = [
    ("id","int","emp_id","int"),
    ("first_name","string","emp_first_name","string"),
    ("last_name","string","emp_last_name","string"),
    ("email","string","emp_email","string"),
    ("gender","string","emp_gender","string"),
    ("ip_address", "string", "emp_ip_address", "string")
]
print("\n\nAPPLY MAPPING")
#Adding Custom Colum Names
df = ApplyMapping.apply(
                    frame=datasource, 
                    mappings=mapping_col,
                    transformation_ctx = "mapping_1")

#Changing to PySpark DataFrame
xf = df.toDF()
#CREATING A TEMPVIEW TO ADD ADDITIONAL COLUMNS AND TO DUMP DATA IN DATABASE
xf.createOrReplaceTempView('df')

data = spark.sql("SELECT *, concat(emp_last_name,', ', emp_first_name) emp_name, '"+year+"' year, '"+month+"' month, '"+day+"' day FROM df")

#CREATING A DYNAMICFRAME TO DUMP DATA
dataframe = DynamicFrame.fromDF(data,glueContext,"dataframe")

#DATALAYER DATA DUMP
sink=glueContext.getSink(connection_type='s3',
                            path=outputPath,
                            enableUpdateCatalog= True,
                            updateBehavior="UPDATE_IN_DATABASE",
                            partitionKeys=["year","month","day"])

#SETTING FILE FORMAT AS GLUEPARQUET
sink.setFormat("glueparquet")
#ESTABLISHING CATALOG DATA FOR DATALAYER
sink.setCatalogInfo(catalogDatabase="emp_database",
                        catalogTableName="emp_data_table")

#DUMPING DATA 
sink.writeFrame(dataframe)

print("\nDATA SUCCESSFULLY DUMPED\n")
print("\n*****JOB FINISHED\n*****\n")
job.commit()

#END