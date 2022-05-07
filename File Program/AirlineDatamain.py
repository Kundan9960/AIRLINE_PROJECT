


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *



if __name__ == '__main__':
    spark=SparkSession.builder\
    .master("local[*]")\
    .appName("Airline Data")\
    .getOrCreate()



    Airline_schema=StructType([StructField("Airline_id",IntegerType()),
                                StructField("Name",StringType()),
                               StructField("Alies",StringType()),
                               StructField("IATA",StringType()),
                               StructField("ICAO",StringType()),
                               StructField("Call_Sign",StringType()),
                               StructField("Country",StringType()),
                               StructField("Active",StringType())

    ])




    df=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\AIRLINE FILES\airline.csv",schema=Airline_schema)

    # df.show()
    # df.printSchema()


    df1=df.na.fill("(Unknown)").na.replace([("\\N")],"(Unknown)")


    '******'

    df1.show()
    df1.printSchema()

    df.na.fill("(Unknown)",["Alies"]).na.replace([("\\N")],"(Unknown)").show()




    # df.withColumn("Alies",when((col("Alies") == "\\N") ,"unknown")).show()
    # df.withColumn("IATA",when((df.IATA.isNull()),"unknown").otherwise(df.IATA)).show()





    df1.write.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Airline",header=True)































