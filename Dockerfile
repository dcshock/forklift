# Forklift server Dockerfile
# 2015-02-20

# Pull base image
FROM dockerfile/java:oracle-java8

# Main developer
MAINTAINER Matt Conroy <elduderino@mailinator.com>

# Update system
RUN apt-get update
RUN apt-get upgrade -y

# Install unzip
RUN apt-get install unzip -y --force-yes --no-install-recommends

# Add forklift server
ADD ./server/target/universal/forklift-server-0.1.zip /tmp/forklift-server-0.1.zip
WORKDIR /tmp
# TODO: Figure out why duplicate JARs are getting put in our dist zip file...
RUN yes | unzip -d /usr/local forklift-server-0.1.zip
#
RUN ln -s /usr/local/forklift-server-0.1 /usr/local/forklift
RUN mkdir -p /usr/local/forklift/consumers
ENV FORKLIFT_HOME /usr/local/forklift
ENV FORKLIFT_CONSUMER_HOME /usr/local/forklift/consumers
ENV PATH $PATH:/usr/local/forklift/bin

# Move to forklift
WORKDIR /usr/local/forklift/

# Start a bash shell
CMD ["bash"]