#!/usr/bin/env python3
'''
	This is the main module for Link-Scout
    @author Kevin Palis <kevin.palis@gmail.com>
'''

from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from util.ls_utility import *
import sys
import getopt
import traceback

def main(argv):
    isVerbose = True
    exitCode = ReturnCodes.SUCCESS
    #initialize spark session
    spark = SparkSession.builder.master("local[*]").appName("LinkScout").getOrCreate()

    path = "../input/part1_in.json"
    res = loadPersonData(spark, path, isVerbose)
    if res is not ReturnCodes.SUCCESS:
        exitWithException(res, spark)

def loadPersonData(spark, filePath, isVerbose):

     # Define schema
    schema = StructType([
        StructField("id",LongType(),False),
        StructField("first",StringType(),False),
        StructField("last",StringType(),False),
        StructField("phone",StringType(),True),
        StructField("experience",ArrayType(StructType([
            StructField('company', StringType(), False),
            StructField('title', StringType(), True),
            StructField('start', DateType(), False),
            StructField('end', DateType(), True)
            ])))
    ])
    # Read json data to dataframe
    df1 = spark.read.option("multiline","true").schema(schema).json(filePath)
    if isVerbose: 
        df1.show()  
        df1.printSchema()
    # Creates a temporary view using the DataFrame
    df1.createOrReplaceTempView("person")
    query = spark.sql("select first, last, experience.company, experience.start FROM person WHERE phone IS NOT null")
    if isVerbose:
        query.show()
    #validations
    idDups = spark.sql("select count(*) - count(distinct id) as ctr from person").first()
    #d = query.rdd.map(lambda p: p.dups).collect()
    print (idDups)
    print (type(idDups.ctr))
    if idDups.ctr > 0:
        return ReturnCodes.ID_DUPLICATED
    #return ReturnCodes.INCOMPLETE_PARAMETERS #test
    else: 
        return ReturnCodes.SUCCESS

def exitWithException(eCode, spark):
    try:
        spark.stop()
        raise LSException(eCode)
    except LSException as e1:
        print("Error code: %s" % e1.code)
        LSUtility.printError(e1.message)
        #traceback.print_exc()
        sys.exit(eCode)

if __name__ == "__main__":
	main(sys.argv[1:])