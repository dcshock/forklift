package forklift.producers;

import javax.jms.Destination;

public interface ProducerInfoI {
    public Destination getDestination();
    public String getBrokerUrl();
}