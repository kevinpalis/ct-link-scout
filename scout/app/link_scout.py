#!/usr/bin/env python3
'''
	This is the main module for Link-Scout: takes in a person_id and searches all connections via two main criteria:
    1. A person is a connection if he/she worked in the same company and their timelines overlapped by at least
        6 months (ie. 182.5 days)
    2. A person is a connection if either one has the others phone number in their list of contacts.

    This utilizes the following libraries/technologies:
    * Pyspark = for all data processing via Apache Spark 
        - the docker container this script comes in with should already provision necessary installations
    * Fuzzywuzzy = for fuzzy matching (ex. using Levenshtein ratio)

    Run the script with -h flag for command-line usage help (ie. python3 link_scout.py -h).
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
    contactsJson = "contacts.json"
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
    #initialize spark session (can be adjusted to accommodate an actual spark cluster)
    spark = SparkSession.builder.master("local[*]").appName("LinkScout").getOrCreate()
    #load the persons data
    res = loadPersonData(spark, personsJson, isVerbose)
    if res is not ReturnCodes.SUCCESS:
        exitWithException(res, spark)
    #load the contacts data
    res2 = loadContactData(spark, contactsJson, isVerbose)
    if res2 is not ReturnCodes.SUCCESS:
        exitWithException(res2, spark)
    if isVerbose:
        showTempTables(spark)
    #criterion 1: get connections by work experience
    connections = set()
    connections = findConnectionsByExp(spark, personId, isVerbose)
    if isVerbose:
        print ("Connections found by work experience: ")
        print (connections)
    #criterion 2: get connections by contacts
    phoneConn = set()
    phoneConn = findConnectionsByContacts(spark, personId, isVerbose)
    #merge 2 results sets, as they are sets, duplicates are removed
    connections.update(phoneConn)
    if isVerbose:
        print ("Connections found by phone number: ")
        print (phoneConn)
        print ("\n")
    #print final connections found
    printConnections(spark, connections, isVerbose)
    if spark is not None: 
            spark.stop()

#prints all connections found oredered by id in this format "id: first last"
def printConnections(spark, connections, isVerbose):
    if len(connections) == 0:
        print("Person has no connections in the dataset.")
        return
    if isVerbose:
        print("Connections found: ")
    conn = spark.sql("select id, first, last from person where id in "+str(tuple(connections))+" order by id")
    #prettier print (easier to look at for bigger datasets):
    #print ("Connections found:")
    #conn.show()
    connList = conn.collect()
    for c in connList:
        print (str(c.id)+": "+c.first+" "+c.last)

#loads contact data from json file
def loadContactData(spark, filePath, isVerbose):
     # Define schema
    schema = StructType([
        StructField("id",LongType(),False),
        StructField("owner_id",LongType(),False),
        StructField("contact_nickname",StringType(),True),
        StructField("phone",ArrayType(StructType([
            StructField('number', StringType(), False),
            StructField('type', StringType(), True)
            ])))
    ])
    try:
        # Read json data to dataframe
        df = spark.read.option("multiline","true").schema(schema).json(filePath)
    except Exception as e:
        print(e)
        return ReturnCodes.ERROR_PARSING_JSON
    df.createOrReplaceTempView("contact")
    #Validations:
    #Check for duplicated id, ie. non-unique entries makes the dataset invalid
    idDups = spark.sql("select count(*) - count(distinct id) as ctr from contact").first()
    if idDups.ctr > 0:
        return ReturnCodes.ID_DUPLICATED
    else: 
        return ReturnCodes.SUCCESS

#loads person data from json file
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
    # Creates a temporary view using the DataFrame
    df1.createOrReplaceTempView("person")
    
    #Validations:
    #Check for duplicated personID, ie. non-unique entries makes the dataset invalid
    idDups = spark.sql("select count(*) - count(distinct id) as ctr from person").first()
    if idDups.ctr > 0:
        return ReturnCodes.ID_DUPLICATED
    else: 
        return ReturnCodes.SUCCESS

#Find connections using contacts data:
#A person is a connection if either one has the others phone number in their list of contacts.
def findConnectionsByContacts(spark, personId, isVerbose):
    personsConnected = set()
    #get the phone number of our person
    fullNum = spark.sql("select phone from person where id ="+personId).first().phone
    #remove country code (as it's irrelevant) and trailing spaces so we can do phone matching
    phoneNum = fullNum.replace("1-", "").strip()
    #get the phone numbers in the contacts list of our person
    ownContacts = spark.sql("select phone from contact where owner_id ="+personId).collect()
    #get the phone numbers in other people's contact list
    othersContacts = spark.sql("select owner_id, phone from contact where owner_id !="+personId).collect()

    #First: look for own phone number's presence in other people's contact list
    if phoneNum is not None:
        for oc in othersContacts:
            for p in oc.phone:
                if phoneNum == normalizePhoneNumber(p["number"]):
                    personsConnected.add(oc.owner_id)
    #Second: look for the id of the people in own's contact list
    for c in ownContacts:
        for q in c.phone:
            match = spark.sql("select id from person where phone like '%"+normalizePhoneNumber(q["number"])+"'").first()
            if match is not None:
                personsConnected.add(match.id)
    return personsConnected

#Find connections using work experience data:
#A person is a connection if he/she worked in the same company and their timelines overlapped by at least 6 months (ie. 182.5 days)
#For each company our person worked for, look through the others' work experience to find fuzzy matches
#Note that we are only taking matches that have a 90% ratio, allowing for some mistakes in spaces and punctuations
def findConnectionsByExp(spark, personId, isVerbose):
    personsConnected = set()
    exp = spark.sql("select experience from person where id ="+personId).first()
    others = spark.sql("select id, experience from person where id !="+personId).collect()

    #iterate through the person's work-experience
    for xp in exp.experience:
        #print("Company: "+xp["company"]+" Date:"+str(xp["start"])+" to "+str(xp["end"])) #debug
        for o in others:
            for x in o.experience:
                #using the ratio (Levenshtein) function here instead of partial_ratio or the token functions - 
                # as rearranging words/tokens would introduce too many false matches for company names
                ratio = fuzz.ratio(xp["company"].lower(),x["company"].lower())
                if ratio >= 90:
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
                        #print("Found a connection: ID="+str(o.id)+" Company="+x["company"]+" Overlap="+str(overlap)+" days") #debug
                        personsConnected.add(o.id)
    return personsConnected

#remove special characters, spaces, and only returns the 10-digit phone number without the country code
def normalizePhoneNumber(num):
    phoneNum = ''.join(i for i in num if i.isalnum())
    return phoneNum[-10:]

#utility method for exception handling
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

#prints usage help
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
		raise LSException(eCode)
	except LSException as e1:
		print (e1.message)
		traceback.print_exc()
		sys.exit(eCode)

#Utility method to pretty print the two tables
def showTempTables(spark):
    print("Person:")
    spark.sql("select * from person").show()
    print("Contact:")
    spark.sql("select * from contact").show()

if __name__ == "__main__":
	main(sys.argv[1:])