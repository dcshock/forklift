package forklift.cluster;

import forklift.consumer.ConsumerThread;
import forklift.ConsumerTest.ExampleJsonConsumer;
import forklift.ConsumerTest.ExpectedMsg;
import forklift.connectors.ConnectorException;
import forklift.consumer.ForkliftConsumerI;
import forklift.producers.ForkliftProducerI;
import forklift.connectors.ForkliftConnectorI;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import forklift.producers.ProducerException;
import forklift.TestMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.Headers;
import forklift.decorators.Message;
import forklift.decorators.Properties;
import forklift.decorators.Queue;
import forklift.decorators.Topic;
import forklift.message.Header;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.JMSException;

import forklift.connectors.ForkliftMessage;
import forklift.decorators.Message;
import forklift.decorators.Topic;

public class ClusterTest {

    private ObjectMapper mapper = new ObjectMapper();
    @Test
    public void CoordinatorTest() {
        Consumer coordinator = new Consumer(Coordinator.class, null, this.getClass().getClassLoader());

        javax.jms.Message jmsMsg = new TestMsg("1");
        ForkliftMessage msg = new ForkliftMessage(jmsMsg);
        msg.setMsg("{}");

        Map<Header, Object> headers = new HashMap<>();
        headers.put(Header.DeliveryCount, "3");
        headers.put(Header.Producer, "testing");
        headers.put(Header.Priority, "1");
        headers.put(Header.CorrelationId, "abcd");
        msg.setHeaders(headers);

        Map<String, Object> properties = new HashMap<>();
        properties.put("my-cool-prop", new Integer(3));
        properties.put("mystrval", "blah");
        properties.put("my-long-val", new Long(123123));
        properties.put("my-float-val", new Float(123123));
        msg.setProperties(properties);

        coordinator.inject(msg, coordinator);

    }

    @Queue("a")
    public class ExampleJsonConsumer {
        @Headers
        Map<Headers, String> headers;

        @Headers
        String cid;

        @Headers(Header.Producer)
        String producer = "replace";

        @Properties
        Map<String, Object> properties;

        @Properties("mystrval")
        String strval = "default";

        @Properties
        String mystrval;

        @Message
        ForkliftMessage fmsg;

        @Message
        Map<String, String> kv;

        @Message
        String str;

        @Message
        ExpectedMsg msg;
    }

    public class FakeConnector implements ForkliftConnectorI {
        @Override
        public void start() throws ConnectorException {
            // TODO Auto-generated method stub

        }

        @Override
        public void stop() throws ConnectorException {
            // TODO Auto-generated method stub

        }

        @Override
        public Connection getConnection() throws ConnectorException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ForkliftConsumerI getQueue(String name) throws ConnectorException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ForkliftConsumerI getTopic(String name) throws ConnectorException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ForkliftProducerI getQueueProducer(String name) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ForkliftProducerI getTopicProducer(String name) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public ForkliftMessage jmsToForklift(javax.jms.Message m) {
            // TODO Auto-generated method stub
            return null;
        }

    }

    public class FakeProducer implements ForkliftProducerI {

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub

        }

        @Override
        public String send(String message) throws ProducerException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String send(ForkliftMessage message) throws ProducerException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String send(Object message) throws ProducerException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String send(Map<String, String> message) throws ProducerException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public String send(Map<Header, Object> headers, Map<String, Object> properties, ForkliftMessage message)
            throws ProducerException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Map<Header, Object> getHeaders() throws ProducerException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void setHeaders(Map<Header, Object> headers) throws ProducerException {
            // TODO Auto-generated method stub

        }

        @Override
        public Map<String, Object> getProperties() throws ProducerException {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void setProperties(Map<String, Object> properties) throws ProducerException {
            // TODO Auto-generated method stub

        }

    }

}
