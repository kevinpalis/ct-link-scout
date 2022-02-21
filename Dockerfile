#author: Kevin Palis <kevin.palis@gmail.com>

#use a lightweight image for pre-build to minimize footprint, this only needs to pull repos
#from alpine/git:v2.30.2 as pre-build
#WORKDIR /toolbox

#prepare IFL and MDE modules
#RUN git clone --progress https://bitbucket.org/ebsproject/gobii-db.git && echo "Gobii-db cloned."
#RUN cp -R gobii-db/dal/gobii_ifl/gobii_ifl/* gobii_bundle/loaders/gobii_ifl && cp -R gobii-db/dal/gobii_mde/gobii_mde/* gobii_bundle/extractors/gobii_mde

FROM ubuntu:20.04
#update and install utility packages, pip, and java
RUN DEBIAN_FRONTEND=noninteractive apt-get update -y && DEBIAN_FRONTEND=noninteractive apt-get install -y \
 sudo \
 wget \
 software-properties-common \
 vim \
 coreutils \
 curl \
 python3-pip \
 default-jre

RUN pip install --upgrade pip
RUN pip install pyspark
EXPOSE 22 4040


#copy the entrypoint/config file and make sure it can execute
COPY entrypoint.sh /root
RUN chmod 755 /root/entrypoint.sh

COPY scout /home/scout
######

ENTRYPOINT ["/root/entrypoint.sh"]
