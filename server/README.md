# README #

## Overview ##
The server project allows developers to embed a forklift server within their applications.

### Embedding with ActiveMQ ###
```java     
 public void startApplication() throws InterruptedException {
        ForkliftOpts options = new ForkliftOpts();
        String ip = "localhost";
        //Note the broker url format:  consule.{activemq-service-name}
        options.setBrokerUrl("consul.activemq-broker");
        options.setConsulHost(ip);
        server = new ForkliftServer(options);
        try {
            server.startServer(10000, TimeUnit.SECONDS);
            // Forklift closes Unirest...we need to reopen it with a call to Options.refresh()
            Options.refresh();
        } catch (InterruptedException e) {
        }
        if (server.getServerState() == ServerState.RUNNING) {
            //Register any Consumers you wish to run in the embedded forklift
            server.registerDeployment(TestStringConsumer.class, TestMapConsumer.class, TestPersonConsumer.class);
        } else {
            try {
                server.stopServer(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            System.exit(-1);
        }
        
        
        public void stopApplication() {
            if(server != null){
                try {
                    server.stopServer(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.error("Forklift shutdown interrupted", e);
                }
            }
        }
    }
```

### Embedding with Kafka and Confluent's Schema-Registry ###
```java

 public void startApplication() throws InterruptedException {
        ForkliftOpts options = new ForkliftOpts();
        String ip = "localhost";
        //Note the broker url format:  consule.{kafka-service-name}.{schema-registry-service-name}
        options.setBrokerUrl("consul.kafka.schema-registry");
        //This will be used as kafka's group name
        options.setApplicationName("MyApplication");
        options.setConsulHost(ip);
        server = new ForkliftServer(options);
        try {
            server.startServer(10000, TimeUnit.SECONDS);
            // Forklift closes Unirest...we need to reopen it with a call to Options.refresh()
            Options.refresh();
        } catch (InterruptedException e) {
        }
        if (server.getServerState() == ServerState.RUNNING) {
            //Register any Consumers you wish to run in the embedded forklift
            server.registerDeployment(TestStringConsumer.class, TestMapConsumer.class, TestPersonConsumer.class);
        } else {
            try {
                server.stopServer(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            System.exit(-1);
        }
        
        
        public void stopApplication() {
            if(server != null){
                try {
                    server.stopServer(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    LOG.error("Forklift shutdown interrupted", e);
                }
            }
        }
    }
```
