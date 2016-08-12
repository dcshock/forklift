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

            KnownInstance k = new KnownInstance(msg.getName(), msg.getGroups(), System.currentTimeMillis());
            if (!HeartbeatThread.state.getKnown().add(k)) {
                // Since we were unable to add the new KnownInstance, we already knew about this producer - instead
                // of creating a new known instance, we must decide how to update the old one

                // This looks totally stupid, because it appears that we're adding and removing the same object
                // However, the equality and hashcode checks in KnownInstance rely only on the name, so we're
                // removing the instance's old version and replacing it with the new. Also this is temporary.
                HeartbeatThread.state.getKnown().remove(k);
                HeartbeatThread.state.getKnown().add(k);
            }

            if (!msg.getName().equals(HeartbeatThread.state.getName()) && nextResponsePermitted.compareTo(System.currentTimeMillis()) > 0) {
                lastResponded = System.currentTimeMillis();
                producer.send(HeartbeatThread.state);
            }
        }
    }

}
