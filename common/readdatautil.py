
class ReadDataUtils:

    def readCsv(self,spark,path,schema=None,inferschema=True,header=True,sep=","):
        """
        Returns new dataframe by reading provided CSV file
        :param spark:spark session object
        :param path: csv file path or directory path
        :param schema:provide schema ,required  when inferschema is false
        :param inferschema: if true :detect file schema else false:ignore auto detect schema
        :param header: if true:input csv file has a header
        :param sep: defult:"," specify separator present in csv file
         :return:
        """

        if (inferschema is False) and (schema==None):
            raise Exception("Please provide inferschema as true or else provide schema for given input file")

        readdf=spark.read.csv(path=path,inferSchema=inferschema,schema=schema,header=header,sep=sep)
        return readdf

    def readParquet(self,spark,path):
        """
        Returns new dataframe by reading provided parquet file
        :param spark: spark session object
        :param path: csv file path or directory path
        :return:
        """

        readparquetdf=spark.read.parquet(path)
        return readparquetdf

    def nullValue(self,df):
        """
        Returns new datafram for null value set default value "unknown" and -1 for int
        :param df: dataframe which want to handle null value
        :return: dataframe after handling null values
        """
        nvl = df.replace(r'\N', 'unknown').fillna(value="unknown").fillna(value=-1)
        return nvl

