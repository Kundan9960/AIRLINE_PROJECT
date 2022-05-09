


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

    # df.filter(df.Airline_id.isNull()).show(1)
    # df.filter(df.Name.isNull()).show(1)
    # df.filter(df.Alies.isNull()).show(10)
    # df.filter(df.IATA.isNull()).show(1)
    # df.filter(df.ICAO.isNull()).show(1)
    # df.filter(df.Call_Sign.isNull()).show(1)
    # df.filter(df.Country.isNull()).show(1)
    # df.filter(df.Active.isNull()).show(1)


    df1=df.na.fill("(Unknown)").na.replace([("\\N")],"(Unknown)")


    '******'

    df1.show()
    df1.printSchema()

    df.na.fill("(Unknown)",["Alies"]).na.replace([("\\N")],"(Unknown)").show()

    # df.filter(df.Airline_id == "\\N").show(1)
    # df.filter(df.Name == "\\N").show(1)
    # df.filter(df.Alies == "\\N").show(10)
    # df.filter(df.IATA == "\\N").show(1)
    # df.filter(df.ICAO == "\\N").show(1)
    # df.filter(df.Call_Sign == "\\N").show(1)
    # df.filter(df.Country == "\\N").show(1)
    # df.filter(df.Active == "\\N").show(1)




    # df.withColumn("Alies",when((col("Alies") == "\\N") ,"unknown")).show()
    # df.withColumn("IATA",when((df.IATA.isNull()),"unknown").otherwise(df.IATA)).show()





    df1.write.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Airline",header=True)































