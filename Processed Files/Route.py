
'''
3. get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport
'''


import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("A").getOrCreate()


    route_df =spark.read.parquet(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\AIRLINE FILES\routes.snappy.parquet")



    # route_df.show()
    # route_df.printSchema()

    # route_df.filter(route_df.codeshare.isNull()).show(1)

    R_df=route_df.na.fill("(Unknown)",["codeshare","equipment"])

    # R_df.show()

    # R_df.filter(R_df.codeshare.isNull()).show(1)


    Route_DF=R_df.withColumn("airline_id", when((col("airline_id") == "\\N"), "unknown").otherwise(R_df.airline_id))\
    .withColumn("dest_airport_id",when((col("dest_airport_id")== "\\N"),"unknown").otherwise(R_df.dest_airport_id))\
    .withColumn("src_airport_id",when((col("src_airport_id")== "\\N"),"unknown").otherwise(R_df.src_airport_id))


    # Route_DF.write.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Route",header=True)

    # Route_DF.show()


    # Airport_df=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Air\Airport.csv",header=True)

    # Air_df=Airport_df.withColumnRenamed("Airport_ID","src_airport_id")
    # Air_df.show()

    # Airport_df.show()
    #
    #
    # A_df= Airport_df.join(Route_DF,Airport_df.Airport_ID==Route_DF.src_airport_id,"inner").select("airline","airline_id")\
    # .groupby("airline","airline_id").count()
    #
    # A_df.where(col("count")>3).show(A_df.count(),False)
    #
    # print(A_df.count())













    # P_Route=Route_DF.groupby("airline_id","src_airport").count()
    #
    # P_Route.show()
    #
    #
    # P_Route.join((Airline_df,Airline_df.Airline_id==P_Route.airline_id),"leftouter").filter(('count')>3).select(Airline_df.Name,P_Route.airline_id).show()







    # Route_DF.write.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Route",header=True)


