from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("Airport").getOrCreate()






    Airport_schema = StructType([StructField("Airport_ID", IntegerType()),
                                 StructField("Name", StringType()),
                                 StructField("City", StringType()),
                                 StructField("Country", StringType()),
                                 StructField("IATA", StringType()),
                                 StructField("ICAO", StringType()),
                                 StructField("Latitude", DoubleType()),
                                 StructField("Longitude", DoubleType()),
                                 StructField("Altitude", IntegerType()),
                                 StructField("Timezone", StringType()),
                                 StructField("DST", StringType()),
                                 StructField("Tz", StringType()),
                                 StructField("Type", StringType()),
                                 StructField("Source", StringType())])

    df1 = spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\AIRLINE FILES\airport.csv",
                     schema=Airport_schema)
    # df1=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\AIRLINE FILES\airport.csv",inferSchema=True)

    df1.show()
    df1.printSchema()

    # df1.filter(df1.Airport_ID.isNull()).show(1)
    # df1.filter(df1.Name.isNull()).show(1)
    # df1.filter(df1.City.isNull()).show(10)
    # df1.filter(df1.Country.isNull()).show(1)
    # df1.filter(df1.IATA.isNull()).show(1)
    # df1.filter(df1.ICAO.isNull()).show(1)
    # df1.filter(df1.Latitude.isNull()).show(1)
    # df1.filter(df1.Longitude.isNull()).show(1)
    # df1.filter(df1.Altitude.isNull()).show(1)
    # df1.filter(df1.Timezone.isNull()).show(1)
    # df1.filter(df1.DST.isNull()).show(1)
    # df1.filter(df1.Tz.isNull()).show(1)
    # df1.filter(df1.Type.isNull()).show(1)
    # df1.filter(df1.Source.isNull()).show(1)

    df2 = df1.na.fill("(Unknown)", ["City"])
    df2.show()
    # df2.filter(df2.City.isNull()).show()

    #
    # df2.filter(df2.Airport_ID == "\\N").show(1)
    # df2.filter(df2.Name == "\\N").show(1)
    # df2.filter(df2.City == "\\N").show(10)
    # df2.filter(df2.Country == "\\N").show(1)
    # df2.filter(df2.IATA == "\\N").show(1)
    # df2.filter(df2.ICAO == "\\N").show(1)
    # df2.filter(df2.Latitude == "\\N").show(1)
    # df2.filter(df2.Longitude == "\\N").show(1)
    # df2.filter(df2.Altitude == "\\N").show(1)
    # df2.filter(df2.Timezone == "\\N").show(1)
    # df2.filter(df2.DST == "\\N").show(1)
    # df2.filter(df2.Tz =="\\N").show(1)
    # df2.filter(df2.Type == "\\N").show(1)
    # df2.filter(df2.Source == "\\N").show(1)




    df3=df2.withColumn("IATA",when((col("IATA")== "\\N"),"(Unknown)").otherwise(df2.IATA))\
    .withColumn("ICAO",when((col("ICAO")== "\\N"),"(Unknown)").otherwise(df2.ICAO))\
    .withColumn("DST",when((col("DST")== "\\N"),"(Unknown)").otherwise(df2.DST))\
    .withColumn("Tz",when((col("Tz")== "\\N"),"(Unknown)").otherwise(df2.Tz))


    # df3.printSchema()
    # df3.show()

    df5= df3.withColumn("Timezone",when((col("Timezone")== "\\N"),"(-1)").otherwise(df2.Timezone))

    df5.printSchema()
    df5.show()

    df5.write.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Air",header=True)








