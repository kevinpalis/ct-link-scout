#!/usr/bin/env python3
'''
	This is the main module for Link-Scout
    @author Kevin Palis <kevin.palis@gmail.com>
'''

from pyspark.sql import SparkSession
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from util.ls_utility import *
from fuzzywuzzy import fuzz
from datetime import datetime, date

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
    personsJson = "persons.json"
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
    #parameters validation
    if int(personId) < 0:
        exitWithException(ReturnCodes.ID_INVALID, None)
    #initialize spark session
    spark = SparkSession.builder.master("local[*]").appName("LinkScout").getOrCreate()

    
    res = loadPersonData(spark, personsJson, isVerbose)
    if res is not ReturnCodes.SUCCESS:
        exitWithException(res, spark)
    
    connectionsList = []
    connectionsList = findConnectionsByExp(spark, personId, isVerbose)
    #debug
    # pid = "1"
    # query = spark.sql("select first, last, experience.company, experience.start FROM person WHERE id ="+pid)
    # if isVerbose:
    #     query.show()

   

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
    try:
        # Read json data to dataframe
        df1 = spark.read.option("multiline","true").schema(schema).json(filePath)
    except Exception as e:
        print(e)
        return ReturnCodes.ERROR_PARSING_JSON

    if isVerbose: 
        df1.show()  
        #df1.printSchema()
    # Creates a temporary view using the DataFrame
    df1.createOrReplaceTempView("person")
    
    #Validations:
    #Check for duplicated personID, ie. non-unique entries makes the dataset invalid
    idDups = spark.sql("select count(*) - count(distinct id) as ctr from person").first()
    #d = query.rdd.map(lambda p: p.dups).collect()
    #print (idDups)
    #print (type(idDups.ctr))
    if idDups.ctr > 0:
        return ReturnCodes.ID_DUPLICATED
    else: 
        return ReturnCodes.SUCCESS

def findConnectionsByExp(spark, personId, isVerbose):
    personsConnected = []
    exp = spark.sql("select experience from person where id ="+personId).first()
    others = spark.sql("select id, first, last, experience from person where id !="+personId).collect()
    #print(others)
    if isVerbose:
        print("Looking for connections using experience: ")
    #iterate through the person's work-experience
    for xp in exp.experience:
        if isVerbose:
            print("Company: "+xp["company"]+" Date:"+str(xp["start"])+" to "+str(xp["end"]))
        #For each company our person worked for, look through the others' work experience to find fuzzy matches
        #Note that we are only taking matches that have a 90% ratio, allowing for some mistakes in spaces and punctuations
        for o in others:
            #print("Checking person #"+str(o.id))
            for x in o.experience:
                #using the ratio (Levenshtein ratio) function here instead of partial_ratio or the token functions - 
                # as rearranging words/tokens would introduce too many false matches for company names
                ratio = fuzz.ratio(xp["company"].lower(),x["company"].lower())
                if ratio >= 90:
                    #print("Found possible match. r="+str(ratio)+"\n\tComparing:"+xp["company"]+ " to "+x["company"])
                    #Found a possible match, now checking if dates in company overlapped
                    #get the latest start date and the earliest end date
                    latestStart = max(xp["start"], x["start"])
                    #set end_dates to current date if null, otherwise, keep value
                    xpEnd = date.today() if xp["end"] is None else xp["end"]
                    xEnd = date.today() if x["end"] is None else x["end"]
                    earliestEnd = min(xpEnd, xEnd)
                    diff = (earliestEnd - latestStart).days + 1
                    overlap = max(0, diff)
                    if overlap > 182: #6 months = 182.5 days
                        if isVerbose:
                            print("Found a connection: ID="+str(o.id)+" Company="+x["company"]+" Overlap="+str(overlap)+" days")
                        personsConnected.append(o.id)
    return personsConnected

def exitWithException(eCode, spark):
    try:
        if spark is not None: 
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