package forklift.activemq.test;

import forklift.connectors.ForkliftMessage;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.decorators.Response;
import forklift.producers.ForkliftResultResolver;

import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

@Queue("response")
public class ResponseConsumerMap {
    static ForkliftResultResolver<Map<String, String>> resolver;

    @forklift.decorators.Message
    private ForkliftMessage m;

    @forklift.decorators.Message
    private Map<String, String> s;

    @OnMessage
    public void go() throws JMSException {
        if (s.containsKey("x")) {
            resolver.resolve(m.getJmsMsg().getJMSCorrelationID(), s);
            throw new RuntimeException("This is expected");
        }
    }

    @Response
    public Map<String, String> response() {
        final Map<String, String> map = new HashMap<>();
        map.put("x", "x");
        return map;
    }
}
