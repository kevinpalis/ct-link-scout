#!/usr/bin/env python3
'''
	Integration tests for LinkScout
    @author Kevin Palis <kevin.palis@gmail.com>
'''
from app import link_scout
from pyspark.sql.session import SparkSession
from app.util.ls_utility import ReturnCodes
import pytest

@pytest.fixture
def spark_session():
    #initialize spark session
    return SparkSession.builder.master("local[*]").appName("LinkScoutIntTest").getOrCreate()

#test looking for connections using work experience criterion
def test_findConnectionsByExperience(spark_session):
    personId = "4"
    personsJson = "test/fixture/persons_load_test.json"
    expectedConnections = {8, 9}
    
    link_scout.loadPersonData(spark_session, personsJson, False)
    connections = link_scout.findConnectionsByExp(spark_session, personId, False)
    
    if spark_session is not None: 
        spark_session.stop()
    assert connections == expectedConnections

#test looking for connections using contacts criterion
def test_findConnectionsByContacts(spark_session):
    contactsJson = "test/fixture/contacts_load_test.json"
    personId = "4"
    personsJson = "test/fixture/persons_load_test.json"
    expectedConnections = {8, 9, 10, 6}
    
    link_scout.loadPersonData(spark_session, personsJson, False)
    link_scout.loadContactData(spark_session, contactsJson, False)
    connections = link_scout.findConnectionsByContacts(spark_session, personId, False)
    
    if spark_session is not None: 
        spark_session.stop()
    assert connections == expectedConnections

#test looking for connections using all criteria
def test_findAllConnections(spark_session):
    contactsJson = "test/fixture/contacts_load_test.json"
    personId = "4"
    personsJson = "test/fixture/persons_load_test.json"
    expectedConnections = {6, 8, 9, 10}

    link_scout.loadPersonData(spark_session, personsJson, False)
    link_scout.loadContactData(spark_session, contactsJson, False)
    connections = link_scout.findConnectionsByExp(spark_session, personId, False)
    phoneConn = link_scout.findConnectionsByContacts(spark_session, personId, False)
    connections.update(phoneConn)
    
    if spark_session is not None: 
        spark_session.stop()
    assert connections == expectedConnections
