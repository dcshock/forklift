# Forklift server Dockerfile
# 2015-02-20

# Pull base image
FROM omriiluz/ubuntu-java8

# Main developer
MAINTAINER Matt Conroy <elduderino@mailinator.com>

# Update system
RUN apt-get update
RUN apt-get upgrade -y

# Install unzip
RUN apt-get install unzip -y --force-yes --no-install-recommends

# Add forklift server
WORKDIR /tmp
# ADD https://github.com/dcshock/forklift/releases/download/0.11/forklift-server-0.11.zip forklift.zip
ADD server/target/universal/forklift-server-0.11.zip forklift.zip
RUN yes | unzip -d /usr/local forklift.zip
RUN ln -s /usr/local/forklift-server-0.11 /usr/local/forklift
RUN mkdir -p /usr/local/forklift/consumers

ENV FORKLIFT_HOME /usr/local/forklift
ENV FORKLIFT_CONSUMER_HOME /usr/local/forklift/consumers
ENV FORKLIFT_PROPS /usr/local/forklift/props
ENV PATH $PATH:/usr/local/forklift/bin

# Move to forklift
WORKDIR /usr/local/forklift/

# Start a bash shell
CMD ["bash"]
