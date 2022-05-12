
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from common.readdatautil import ReadDataUtils
from common.airlineschema import *
from pyspark.sql.functions import *


def printAllFileData(*arg):
    for i in arg:
        i.show(truncate=False)


def printAllFileSchema(*args):
    for i in args:
        i.printSchema()

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Airline Data Management").getOrCreate()
    rdu = ReadDataUtils()
    airlineschema=AirlineSchema

    #path
    """
    Declare all file path here
    """
    airlinepath = r"C:\Data engineer\Assignment\Spark\Spark project\input\airline data\airline.csv"
    airportpath = r"C:\Data engineer\Assignment\Spark\Spark project\input\airline data\airport.csv"
    planepath = r"C:\Data engineer\Assignment\Spark\Spark project\input\airline data\plane.csv.gz"
    routepath = r"C:\Data engineer\Assignment\Spark\Spark project\input\airline data\routes.snappy.parquet"

    #schema
    """
    Declare Schema for all files here
    """
    schema = airlineschema.airlinecsvschema
    airportschema = airlineschema.airportcsvschema
    planeschema = airlineschema.planeschema

    # Reading airline.csv
    # print("------Reading airline.csv file-------")
    airdf = rdu.readCsv(spark=spark,path=airlinepath,schema=schema,header=False)
    airlinedf=rdu.nullValue(airdf)


    #Reading airport.csv
    # print("------Reading airport.csv file-------")
    portdf = rdu.readCsv(spark=spark,path=airportpath,header=False,schema=airportschema)
    airportdf=rdu.nullValue(portdf)
    # printAllFileData(airportdf)


    # #Reading plane.csv.gz
    # print("------Reading plane.csv.gz file-------")
    planedf=rdu.readCsv(spark=spark,path=planepath,schema=planeschema,sep="")
    airplanedf = rdu.nullValue(planedf)
    # printAllFileData(airplanedf)


    # #Reading route.snappy.parquet file
    # print("------Reading route.snappy.parquet file------")
    df=rdu.readParquet(spark,path=routepath)
    route=df.withColumn('airline_id',col('airline_id').cast('int')).withColumn('src_airport_id',col('src_airport_id').cast('int')).withColumn('dest_airport_id',col('dest_airport_id').cast('int'))
    airroutedf= rdu.nullValue(route)
    # printAllFileData(airroutedf)

    #Print all file schema and data together
    # printAllFileSchema(airlinedf, airportdf,airplanedf,airroutedf)
    # printAllFileData(airlinedf, airportdf,airplanedf,airroutedf)


    #Q2.find the country name which is having both airlines and airport
    # ---Using spark df
    # airlinedf.join(airportdf,airlinedf.country==airportdf.country,"inner").select(airlinedf.country).distinct().show()

    #---Using spark SQL
    # airlinedf.createOrReplaceTempView("Airline")
    # airportdf.createOrReplaceTempView("Airport")
    # spark.sql("select distinct al.country from Airline al ,Airport ap where al.country==ap.country").show()


    """
    3. get the airlines details like name, id,. which is has taken takeoff more than 3 times from same airport 
    """
    # takeoffcount=airroutedf.select("src_airport").groupby("src_airport").count().alias("count").filter(col("count")>3)
    # # takeoffcount.show()
    #
    # airroute=airroutedf.join(takeoffcount,airroutedf.src_airport==takeoffcount.src_airport,"inner").drop(takeoffcount.src_airport)
    #
    # airlinedf.join(airroute,airlinedf.airline_id==airroute.airline_id,"inner").select(
    #     airlinedf.airline_id,"name").distinct().show(truncate=False)

    """
    3. get airport details which has minimum number of takeoffs and landing.
    """
    # #finding minimum takeoff
    # mintake=airroutedf.groupby("src_airport_id").count().withColumnRenamed("count","mintakeoff")
    # # mintake.show()
    # m=mintake.agg(min("mintakeoff")).take(1)[0][0]
    # takeoff=mintake.select("src_airport_id").filter(col("mintakeoff")==m)
    # # takeoff.show()
    #
    # # finding minimum landing
    # minland = airroutedf.groupby("dest_airport_id").count().withColumnRenamed("count", "minlanding")
    # # minland.show()
    # ml = minland.agg(min("minlanding")).take(1)[0][0]
    # landing = minland.select("dest_airport_id").filter(col("minlanding") == ml)
    # # landing.show()
    #
    # #merge minimum takeoff and minimum landing
    # takeofflanding=landing.union(takeoff).distinct()
    # # takeofflanding.show()
    #
    # #dispaly airport details which has minimum number of takeoffs and landing
    # airportdf.join(takeofflanding,airportdf.airline_id==takeofflanding.dest_airport_id,"inner").drop("dest_airport_id").show(truncate=False)
    #
    """
    4. get airport details which is having maximum number of takeoff and landing.
    """
    # # finding maximum takeoff
    # maxtake = airroutedf.groupby("src_airport_id").count().withColumnRenamed("count", "maxtakeoff")
    # # maxtake.show()
    # mt = maxtake.agg(max("maxtakeoff")).take(1)[0][0]
    # # print(mt)
    # mxtakeoff = maxtake.select("src_airport_id").filter(col("maxtakeoff") == mt)
    # # mxtakeoff.show()
    #
    # # finding maximum landing
    # maxland = airroutedf.groupby("dest_airport_id").count().withColumnRenamed("count", "maxlanding")
    # # maxland.show()
    # mxl = maxland.agg(max("maxlanding")).take(1)[0][0]
    # # print(mxl)
    # mxlanding = maxland.select("dest_airport_id").filter(col("maxlanding") == mxl)
    # # mxlanding.show()
    #
    # # # merge maximum takeoff and maximum landing
    # mxtakeofflanding = mxtakeoff.union(mxlanding).distinct()
    # # mxtakeofflanding.show()
    # #
    # # dispaly airport details which has maximum number of takeoffs and landing
    # airportdf.join(mxtakeofflanding, airportdf.airline_id == mxtakeofflanding.src_airport_id, "inner").drop(
    #     "src_airport_id").show(truncate=False)

#5. Get the airline details, which is having direct flights. details like airline id, name, source airport name, and destination airport name
    # directfightdf = airroutedf.select("airline_id",col("src_airport").alias("source_airport_name"),col("dest_airport").alias("destination_airport_name")).filter(col("stops") == 0).distinct()
    #
    # airlinedf.select("airline_id","name").join(directfightdf,airlinedf.airline_id==directfightdf.airline_id,"inner").drop(directfightdf.airline_id).sort(desc(airlinedf.airline_id)).show()
    #

    #----using Spark SQL
    # airlinedf.createOrReplaceTempView("Airline")
    # airroutedf.createOrReplaceTempView("Airroute")
    # spark.sql("select distinct al.airline_id,al.name,ar.src_airport as source_airport_name,ar.dest_airport as destination_airport_name from Airline al,Airroute ar where al.airline_id==ar.airline_id and ar.stops=0 order by al.airline_id desc").show()

