
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
    #
    # airlinedf.join(airroute,airlinedf.airline_id==airroute.airline_id,"inner").select(
    #     airlinedf.airline_id,"name").distinct().show()

    """
    3. get airport details which has minimum number of takeoffs and landing.
    4. get airport details which is having maximum number of takeoff and landing.
    """

    airroutedf.withColumn("mintakeoff", min("src_airport_id").over(Window.partitionBy("src_airport"))).select("airline_id","src_airport_id","src_airport").distinct().show()
    # airroutedf.select("src_airport_id","src_airport").distinct().withColumn("maxtakeoff", max("src_airport_id").over(Window.partitionBy("src_airport"))).show()
    # #
    # airroutedf.select("dest_airport_id","dest_airport").distinct().withColumn("minlanding", min("dest_airport_id").over(Window.partitionBy("dest_airport"))).show()
    # airroutedf.select("dest_airport_id","dest_airport").distinct().withColumn("maxlanding", max("dest_airport_id").over(Window.partitionBy("dest_airport"))).show()

    # airroutedf.createOrReplaceTempView("airroute")
    # airlinedf.createOrReplaceTempView("airline")
    # spark.sql("select ar.airline_id,ar.src_airport_id,count(ar.src_airport_id) from airroute ar,airline al group by ar.airline_id,ar.src_airport_id having count(ar.src_airport_id)>=3").show()


    #5. Get the airline details, which is having direct flights. details like airline id, name, source airport name, and destination airport name
    # directfightdf = airroutedf.select("airline_id",col("src_airport").alias("source_airport_name"),col("dest_airport").alias("destination_airport_name")).filter(col("stops") == 0).distinct()
    #
    # airlinedf.select("airline_id","name").join(directfightdf,airlinedf.airline_id==directfightdf.airline_id,"inner").drop(directfightdf.airline_id).sort(desc(airlinedf.airline_id)).show()
    #

    #----using Spark SQL
    # airlinedf.createOrReplaceTempView("Airline")
    # airroutedf.createOrReplaceTempView("Airroute")
    # spark.sql("select distinct al.airline_id,al.name,ar.src_airport as source_airport_name,ar.dest_airport as destination_airport_name from Airline al,Airroute ar where al.airline_id==ar.airline_id and ar.stops=0 order by al.airline_id desc").show()

