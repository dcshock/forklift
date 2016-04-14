package forklift.activemq.test;

import forklift.connectors.ForkliftMessage;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.decorators.Response;
import forklift.producers.ForkliftResultResolver;

import javax.jms.JMSException;

@Queue("response")
public class ResponseConsumerObj {
    static ForkliftResultResolver<ResponseObj> resolver;

    @forklift.decorators.Message
    private ForkliftMessage m;

    @forklift.decorators.Message
    private ResponseObj s;

    @OnMessage
    public void go() throws JMSException {
        if (s.getName() != null && s.getName().equals("Dude") && s.getAge() != null && s.getAge() == 22) {
            resolver.resolve(m.getJmsMsg().getJMSCorrelationID(), s);
            throw new RuntimeException("This is expected");
        }
    }

    @Response
    public ResponseObj response() {
        final ResponseObj o = new ResponseObj();
        o.setName("Dude");
        o.setAge(22);
        return o;
    }
}
