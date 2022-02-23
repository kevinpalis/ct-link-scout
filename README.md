# Link Scout #

This is a simple application that takes in a **person_id** and searches all connections via two main criteria:

1. A person is a connection if he/she worked in the same company and their timelines overlapped by at least 6 months (ie. 182.5 days)
2. A person is a connection if either one has the others phone number in their list of contacts.

The data comes in the form of 2 JSON files: persons.json and contacts.json.

## Libraries required (pre-provisioned)

This application utilizes the following libraries/technologies:

- Pyspark = for all data processing via Apache Spark 
- Fuzzywuzzy = for fuzzy matching (ex. using Levenshtein ratio)
- Pytest = for unit tests and integration tests

> The docker container this application comes in with should already provision necessary installations. See **Dockerfile**.
>
> As such, the only real requirement is that you have the **docker engine**.

## Running the application

There are two ways to run this application, in the order of preference:

### Run using the pre-built docker image

There is a pre-built docker image in Dockerhub ( **gadm01/link-scout** ) - which means you don't even need to pull this repository. In a command line run the following:

```bash  
#Pull the docker image
docker pull gadm01/link-scout

#Run the docker image
docker run --detach --name link-scout gadm01/link-scout

#Go inside the container's shell to run the CLI of link-scout
docker exec -ti link-scout bash
```
Once you're inside the container, everything will be provisioned for you so you can simply use link-scout's CLI. Here are sample commands:

```bash  
#Navigate to link-scout's root directory:
cd /home/link-scout
#Find all connections of person with id=4 (using default json paths, persons.json and contacts.json):
python3 link_scout.py -i 4

#Find all connections of person with id=3 with verbose printing:
python3 link_scout.py -i 3 -v

#Find all connections of person with id=2 and passing a new persons.json file:
python3 link_scout.py -p "test/fixture/persons_load_test.json" -i 2

#Print usage help
python3 link_scout.py -h
```

#### Command line usage

### Run by building from source (ie. docker build)

If you really want to build from source, here are the steps:

## Running the tests

Simply run pytest to run both integration tests and unit tests:

## Questions/Clarifications ###
Please contact:

* **Kevin Palis** <kevin.palis@gmail.com>