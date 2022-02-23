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

#Run the docker container
docker run --detach --name link-scout -it gadm01/link-scout

#Go inside the container's shell to run the CLI of link-scout
docker exec -ti link-scout bash
```

Once you're inside the container, everything will be provisioned for you so you can simply use link-scout's CLI. Here are sample commands:

```bash  
#Navigate to link-scout's root directory:
cd /home/scout/app
#Find all connections of person with id=4 (using default json paths, persons.json and contacts.json):
python3 link_scout.py -i 4

#Find all connections of person with id=3 with verbose printing:
python3 link_scout.py -i 3 -v

#Find all connections of person with id=2 and passing a new persons.json file:
python3 link_scout.py -p "test/fixture/persons_load_test.json" -i 2

#Print usage help
python3 link_scout.py -h
```

### Run by building from source (ie. docker build)

If you really want to build from source, here are the steps:

```bash  
#Make sure you are in the same directory as the Dockerfile, then run
docker build -t link-scout .

#Run the docker container
docker run --detach --name link-scout -it link-scout

#Go inside the container's shell to run the CLI of link-scout
docker exec -ti link-scout bash
```
Once you're inside the container, you can use link-scout's CLI as shown in the previous section.

## Running the tests

Assuming all steps before this section was successful, you can simply run pytest to run both integration tests and unit tests:

```bash  
#Go to application's home
cd /home/scout/app
#To run all tests:
python3 -m pytest
#To run just unit tests:
python3 -m pytest test/ls_unit_test.py
#To run just integration tests:
python3 -m pytest test/ls_integration_test.py
```

### Areas of improvement
- More unit tests and integration tests
- Bigger test files of varying use cases

### Questions/Clarifications ###
Please contact:

* **Kevin Palis** <kevin.palis@gmail.com>