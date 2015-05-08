package forklift.producers;

import forklift.producers.ProducerInfoI;

import javax.jms.Destination;

public class ActiveMQProducerInfo implements ProducerInfoI {
    
    private Destination destination;
    private String brokerUrl;

    @Override
    public Destination getDestination() {
        return this.destination;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }
    
    @Override
    public String getBrokerUrl() {
        return this.brokerUrl;
    }

    public void setBrokerUrl(String url) {
        this.brokerUrl = url;
    }
}