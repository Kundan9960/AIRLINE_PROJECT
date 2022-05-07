# '''''
# 1. In any of your input file if you are getting "\N" or null values in your column and
# that column is of string type then put default value as "(unknown)" and
# if column is of type integer then put -1
# '''''

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window

if __name__ == '__main__':
      spark=SparkSession.builder.master("local[*]").appName("Excersice").getOrCreate()


Airline_df=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Air\Airline.csv",header=True)



Airline_df.show()
# Airline_df.printSchema()

Airport_df=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Air\Airport.csv",header=True)

Airport_df.show()
# Airport_df.printSchema()

Route_df=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Air\Route.csv",header=True)

# Route_df.show()
# Route_df.printSchema()

Plane_df=spark.read.csv(r"C:\Users\kagar\PycharmProjects\Pyspark Airline Project\OUTPUT\Air\Plane.csv",header=True)

# Plane_df.show()
# Plane_df.printSchema()



'''
2. find the country name which is having both airlines and airport

'''


# Airline_df.join(Airport_df, Airline_df.Country == Airport_df.Country, "inner").select("City").distinct().show(5000)

# df.join(df1, df.Country == df1.Country, "inner").select("City").show(5000)


'''
3. get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport
'''


# A_df = Airport_df.join(Route_df, Airport_df.Airport_ID == Route_df.src_airport_id, "inner").select("airline","airline_id") \
# .groupby("airline", "airline_id").count()

# A_df.where(col("count") > 3).show(A_df.count(), False)



# A_df = Airport_df.join(Route_df, Airport_df.Airport_ID == Route_df.src_airport_id, "inner").select("airline","airline_id") \
# .groupby("airline", "airline_id").count().where(col("count") > 3).distinct()

# A_df.show()

# print(A_df.count())




#
# print(A_df.count())


'''
3.get airport details which has minimum number of takeoffs and landing.
'''

''' Minimum No Of Takeoffs:-'''
# Joining Airport & Route on the Basis of "Airport_ID" * and Group by it..Save it On New DF.with Count column


Take_off=Airport_df.join(Route_df, Airport_df.Airport_ID == Route_df.src_airport_id, "inner").select("airline","airline_id","src_airport") \
.groupby("src_airport").count()



#
# # Applying Window function on it
#
windos=Window.partitionBy("count").orderBy(col("count").asc())

take_off_df=Take_off.withColumn("Rank",rank().over(windos))

take_off_df.show()

# t1=take_off_df.groupby("src_airport").agg(min(col("count")))
# t1.orderBy(t1.min(count).asc()).show()

# # t1.orderBy(take_off_df.min(count).asc()).show()






'''
Minimun No Of Landing:-.
'''

# Joining Airport_df and Route_df on the basis Of Airport_ID And Group By It AND save It On NEW_df


# Landing=Airport_df.join(Route_df, Airport_df.Airport_ID == Route_df.dest_airport_id, "inner").select("dest_airport","airline_id") \
# .groupby("airline_id", "dest_airport").count()


# windos=Window.partitionBy("count").orderBy("count")

# landing_df=Landing.withColumn("Rank",rank().over(windos)).distinct()

# landing_df.show()





'''
4. get airport details which is having maximum number of takeoff and landing.
'''

''' MAXIMUM NUMBERS OF TAKE-OFF'''


# Max_Take_off=Airport_df.join(Route_df, Airport_df.Airport_ID == Route_df.src_airport_id, "inner").select("airline","airline_id","src_airport") \
# .groupby("src_airport").count()

# Max_Take_off.show()

# Applying Window function on it

# windos=Window.orderBy(col("count").desc())
#
# max_take_off_df=Max_Take_off.withColumn("Rank",rank().over(windos)).distinct()

# max_take_off_df.show()



''' MAXIMUM NUMBER OF LANDING:-'''


# M_Landing=Airport_df.join(Route_df, Airport_df.Airport_ID == Route_df.dest_airport_id, "inner").select("dest_airport","airline_id") \
# .groupby("dest_airport").count()
#
#
# windos=Window.orderBy(col("count").desc())
#
# MAX_landing_df=M_Landing.withColumn("Rank",rank().over(windos)).distinct()

# MAX_landing_df.show()
#








'''
5. Get the airline details, which is having direct flights.
 details like airline id, name, source airport name, and destination airport name
'''

renamed_df=Airline_df.withColumnRenamed("Airline_id","airline_id")

#### renamed_df.show()


proccesed_id_df=renamed_df.join(Route_df, on="airline_id" , how="leftouter").select("Airline_id",col("Name").alias("Airline_name"),"src_airport","src_airport_id","dest_airport_id","stops")\
.where(col("stops")==0).distinct()

# proccesed_id_df.show()

src_name_df=proccesed_id_df.join(Airport_df, proccesed_id_df.src_airport_id==Airport_df.Airport_ID,"inner")\
.select("Airline_id","Airline_name",col("Name").alias("src_airport") ,"dest_airport_id","stops").distinct()

# src_name_df.orderBy("Airline_id").show()

Final_details=src_name_df.join(Airport_df ,src_name_df.dest_airport_id==Airport_df.Airport_ID,"inner")\
.select("Airline_id","Airline_name","src_airport",col("Name").alias("dest_Airport"),"stops").distinct()
#
Final_details.orderBy("Airline_id").show()














