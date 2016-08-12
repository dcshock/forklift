package forklift.cluster;

import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.decorators.Producer;
import forklift.decorators.Topic;
import forklift.producers.ForkliftProducerI;
import forklift.producers.ProducerException;

@Topic("forklift.coordinator")
public class Coordinator {
    @Producer(topic="forklift.coordinator")
    private ForkliftProducerI producer;

    @Message
    private CoordinationState msg;

    private Long lastResponded = 0L;

    @OnMessage
    public void onMessage() throws ProducerException {
        System.out.println(msg);

        synchronized (HeartbeatThread.state) {
            Long nextResponsePermitted = lastResponded + 1000;

            HeartbeatThread.state.getKnown().put(msg.getName(), msg.getGroups());

            if (!msg.getName().equals(HeartbeatThread.state.getName()) && nextResponsePermitted.compareTo(System.currentTimeMillis()) > 0) {
                lastResponded = System.currentTimeMillis();
                HeartbeatThread.state.getKnown().put(msg.getName(), msg.getGroups());
                producer.send(HeartbeatThread.state);
            }
        }
    }

}
