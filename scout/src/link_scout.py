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

#Temporary Views:
# Person = data from persons.json in table form
# Contact = data from contacts.json in table form

def main(argv):
    #defaults
    isVerbose = False
    personsJson = ""
    contactsJson = ""
    personId = -1
    exitCode = ReturnCodes.SUCCESS
    
    #Get and parse parameters
    try:
        opts, args = getopt.getopt(argv, "hp:c:i:v", ["personsJson=", "contactsJson=", "personId=", "verbose"])
        #print (opts, args)
        # No arguments supplied, show help
        if len(args) < 1 and len(opts) < 1:
            printUsageHelp(ReturnCodes.SUCCESS)
    except getopt.GetoptError:
        # print ("OptError: %s" % (str(e1)))
        exitWithException(ReturnCodes.INVALID_OPTIONS, None)
    for opt, arg in opts:
        if opt == '-h':
            printUsageHelp(ReturnCodes.SUCCESS)
        elif opt in ("-p", "--personsJson"):
            personsJson = arg
        elif opt in ("-c", "--contactsJson"):
            contactsJson = arg
        elif opt in ("-i", "--personId"):
            personId = arg
        elif opt in ("-v", "--verbose"):
            isVerbose = True

    #initialize spark session
    spark = SparkSession.builder.master("local[*]").appName("LinkScout").getOrCreate()

    personsJson = "../input/part1_in.json"
    res = loadPersonData(spark, personsJson, isVerbose)
    query = spark.sql("select first, last, experience.company, experience.start FROM person WHERE id = 1")
    if isVerbose:
        query.show()

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
    pid = "1"
    query = spark.sql("select first, last, experience.company, experience.start FROM person WHERE id ="+pid)
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

def printUsageHelp(eCode):
	print (eCode)
	print ("python3 link_scout.py -p <personsJson:string> -c <contactsJson:string> -i <personId:int>")
	print ("\t-h = Usage help")
	print ("\t-p or --personsJson = (OPTIONAL) Path to the Json file with persons data. Default (if unset): persons.json in current directory")
	print ("\t-c or --contactsJson = (OPTIONAL) Path to the Json file with contacts data. Default (if unset): contacts.json in current directory")
	print ("\t-i or --personId = The ID of the person you want to search connections of.")
	print ("\t-v or --verbose = (OPTIONAL) Print the status of LS execution in more detail.")
	if eCode == ReturnCodes.SUCCESS:
		sys.exit(eCode)
	try:
		raise GQLException(eCode)
	except GQLException as e1:
		print (e1.message)
		traceback.print_exc()
		sys.exit(eCode)

if __name__ == "__main__":
	main(sys.argv[1:])