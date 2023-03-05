import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time

appName= "hive_pyspark"
master= "local"

S3_DATA_SOURCE_PATH='s3://delayflightbucket/dataset/DelayedFlights-updated.csv'
S3_DATA_OUTPUT_PATH_DF1='s3://delayflightbucket/output/df1'
S3_DATA_OUTPUT_PATH_DF2='s3://delayflightbucket/output/df2'
S3_DATA_OUTPUT_PATH_DF3='s3://delayflightbucket/output/df3'
S3_DATA_OUTPUT_PATH_DF4='s3://delayflightbucket/output/df4'
S3_DATA_OUTPUT_PATH_DF5='s3://delayflightbucket/output/df5'

def main():
    spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
    datafile=spark.read.csv('s3://delayflightbucket/dataset/DelayedFlights-updated.csv', header=True)
    print('===Total data count in source file: %s'% datafile.count())

    datafile.show(100)
    datafile.write.mode('overwrite').saveAsTable("delays")

    start_time1 = time.time()
    df1=spark.sql('select Year,CarrierDelay from delays where Year>2002 AND Year<2011 ORDER BY Year Limit 1001')
    print(f"Execution time: {time.time() - start_time1}")
    df1.show(1001)
    df1.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_DF1)

    df2=spark.sql('select Year,NASDelay from delays where Year>2002 AND Year<2011 ORDER BY Year Limit 1001')
    df2.show(1001)
    df2.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_DF2)

    df3=spark.sql('select Year,WeatherDelay from delays where Year>2002 AND Year<2011 ORDER BY Year Limit 1001')
    df3.show(1001)
    df3.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_DF3)

    df4=spark.sql('select Year,LateAircraftDelay from delays where Year>2002 AND Year<2011 ORDER BY Year Limit 1001')
    df4.show(1001)
    df4.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_DF4)

    df5=spark.sql('select Year,SecurityDelay from delays where Year>2002 AND Year<2011 ORDER BY Year Limit 1001')
    df5.show(1001)
    df5.write.mode('overwrite').parquet(S3_DATA_OUTPUT_PATH_DF5)

if __name__ == '__main__':
    main()




