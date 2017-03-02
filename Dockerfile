FROM gliderlabs/alpine:3.3
MAINTAINER Matt Conroy <elduderino@mailinator.com>

# Install java 8
RUN apk add --no-cache openjdk8

# Install bash so that the server can actually run.
RUN apk add --no-cache bash

# Add forklift server
WORKDIR /tmp
ADD server/target/universal/forklift-server-0.33.zip forklift.zip
RUN yes | unzip -d /usr/local forklift.zip
RUN ln -s /usr/local/forklift-server-0.33 /usr/local/forklift

RUN rm forklift.zip
RUN mkdir -p /usr/local/forklift/consumers

ENV FORKLIFT_HOME /usr/local/forklift
ENV FORKLIFT_CONSUMER_HOME /usr/local/forklift/consumers
ENV FORKLIFT_PROPS /usr/local/forklift/props
ENV PATH $PATH:/usr/local/forklift/bin

# Move to forklift
WORKDIR /usr/local/forklift/

# Start a bash shell
CMD ["sh"]
