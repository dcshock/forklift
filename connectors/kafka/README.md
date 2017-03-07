# README #

## Kafka Connector ##
The kafka connector allows the forklift to utilize kafka and confluent's schema registry.  

## JMS ##
Forklift was developed against the JMS spec.  Kafka is not a JMS implementation so some adaptation
has occurred in order to allow Kafka to function with Forklift.

### Queues and Topics ###
Kafka only provides one form of topic which is analogous to ActiveMQ's Virtual Topic.  Only one member from
each subscribed group will receive a message.  Requests for a queue or topic will return the 
same producer type.

### At Most Once Delivery ###
The kafka connector does not guarantee at most once delivery although it makes a best effort. 
In order to adapt to the JMS spec, messages are acknowledged and added to a pending commit batch, 
which are committed to kafka every poll cycle.  It is therefore possible for the server to crash 
before the acknowledgment batch has been committed, and after the message has processed.


