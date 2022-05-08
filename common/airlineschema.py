from pyspark.sql.types import *

class AirlineSchema:

    airportcsvschema = StructType([StructField("airline_id", IntegerType()),
                                StructField("name", StringType()),
                                StructField("city", StringType()),
                                StructField("country", StringType()),
                                StructField("iata", StringType()),
                                StructField("icao", StringType()),
                                StructField("latitude", FloatType()),
                                StructField("longitude", FloatType()),
                                StructField("altitude", IntegerType()),
                                StructField("timezone", FloatType()),
                                StructField("dst", StringType()),
                                StructField("tz", StringType()),
                                StructField("type", StringType()),
                                StructField("source", StringType()),
                                ])

    airlinecsvschema = StructType([StructField("airline_id", IntegerType()),
                       StructField("name", StringType()),
                       StructField("alias", StringType()),
                       StructField("iata", StringType()),
                       StructField("icao", StringType()),
                       StructField("callsign", StringType()),
                       StructField("country", StringType()),
                       StructField("active", StringType())
                       ])

    planeschema = StructType([StructField("name", StringType()),
                            StructField("iata_code",StringType()),
                            StructField("icao_code", StringType())
                            ])

    routeschema = StructType([StructField("airline", StringType()),
                            StructField("airline_id",IntegerType()),
                            StructField("source_airport",StringType()),
                            StructField("source_airport_id", IntegerType()),
                            StructField("destination_airport",StringType()),
                            StructField("destination_airport_id",IntegerType()),
                            StructField("codeshare",StringType()),
                            StructField("stops",IntegerType()),
                            StructField("equipment",StringType())
                            ])

