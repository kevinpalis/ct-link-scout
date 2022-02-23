#!/usr/bin/env python3
'''
	Unit tests for LinkScout
    @author Kevin Palis <kevin.palis@gmail.com>
'''
from app import link_scout
from pyspark.sql.session import SparkSession
from app.util.ls_utility import ReturnCodes
import pytest

@pytest.fixture
def spark_session():
    #initialize spark session
    return SparkSession.builder.master("local[*]").appName("LinkScoutTest").getOrCreate()

#test normalizing phone numbers with special characters and return only the 10-digit phone number
def test_normalizePhoneNumber1():
    assert link_scout.normalizePhoneNumber('(212) 345-8974') == "2123458974"

#test normalizing phone number with country code and return only the 10-digit phone number
def test_normalizePhoneNumber2():
    assert link_scout.normalizePhoneNumber('+1 4443473475') == "4443473475"

#test loading a valid persons.json
def test_loadPersonData(spark_session):
    filePath = "test/fixture/persons_load_test.json"
    ret = link_scout.loadPersonData(spark_session, filePath, False)
    if spark_session is not None: 
        spark_session.stop()
    assert ret == ReturnCodes.SUCCESS

#test loading an invalid persons.json - person.id duplicated
def test_loadPersonData_duplicatedID(spark_session):
    filePath = "test/fixture/persons_load_test_dup_id.json"
    ret = link_scout.loadPersonData(spark_session, filePath, False)
    if spark_session is not None: 
        spark_session.stop()
    assert ret == ReturnCodes.ID_DUPLICATED

#test loading a valid contacts.json
def test_loadContactData(spark_session):
    filePath = "test/fixture/contacts_load_test.json"
    ret = link_scout.loadContactData(spark_session, filePath, False)
    if spark_session is not None: 
        spark_session.stop()
    assert ret == ReturnCodes.SUCCESS

#test loading an invalid contacts.json - contact.id duplicated
def test_loadContactData_duplicatedID(spark_session):
    filePath = "test/fixture/contacts_load_test_dup_id.json"
    ret = link_scout.loadContactData(spark_session, filePath, False)
    if spark_session is not None: 
        spark_session.stop()
    assert ret == ReturnCodes.ID_DUPLICATED