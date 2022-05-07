from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("Plane").getOrCreate()


Plane_df=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\AIRLINE FILES\plane.csv",sep="",header=True)

Plane_df.printSchema()
Plane_df.show(truncate=False)


Plane_df.filter(col("IATA Code").isNull()).show(1)
Plane_df.filter(col("Name").isNull()).show(1)
Plane_df.filter(col("ICAO Code").isNull()).show(1)

# Plane_df.filter(col("IATA Code") == "\\N").show(1)
# Plane_df.filter(col("Name") == "\\N").show(1)
# Plane_df.filter(col("ICAO Code") == "\\N").show(1)



df3=Plane_df.withColumn("IATA Code",when((col("IATA Code")== "\\N"),"(Unknown)").otherwise(col("IATA Code")))\
    .withColumn("ICAO Code",when((col("ICAO Code")== "\\N"),"(Unknown)").otherwise(col("ICAO Code")))\

df3.show()






# df3.write.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Plane",header=True)

