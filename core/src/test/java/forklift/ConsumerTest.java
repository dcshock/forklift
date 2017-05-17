package forklift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import forklift.connectors.ConnectorException;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.Consumer;
import forklift.decorators.Headers;
import forklift.decorators.Message;
import forklift.decorators.Properties;
import forklift.message.Header;
import forklift.source.decorators.Queue;
import forklift.source.decorators.Topic;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ConsumerTest {

    private Forklift forklift;
    private ForkliftConnectorI connector;

    @Before
    public void setup() {
        forklift = mock(Forklift.class);
        connector = mock(ForkliftConnectorI.class);
        when(forklift.getConnector()).thenReturn(connector);
    }

    @Test(expected = IllegalArgumentException.class)
    public void createBadConsumer() {
        new Consumer(BadConsumer.class, forklift, this.getClass().getClassLoader());
    }

    @Test(expected = IllegalArgumentException.class)
    public void createDoubleConsumer() {
        new Consumer(DoubleConsumer.class, forklift, this.getClass().getClassLoader());
    }

    @Test
    public void createQueueConsumer() {
        Consumer c = new Consumer(QueueConsumer.class, forklift, this.getClass().getClassLoader());
        assertTrue(c.getName().matches("abc:\\d+"));
    }

    @Test
    public void createTopicConsumer() {
        Consumer c = new Consumer(TopicConsumer.class, forklift, this.getClass().getClassLoader());
        assertTrue(c.getName().matches("xyz:\\d+"));
    }

    @Test
    public void inject() throws ConnectorException {
        Consumer test = new Consumer(ExampleConsumer.class, forklift, this.getClass().getClassLoader());
        ExampleConsumer ec = new ExampleConsumer();
        ForkliftMessage msg = new ForkliftMessage();
        msg.setId("1");
        msg.setMsg("x=y\nname=Scooby Doo\n");

        test.inject(msg, ec);

        // Now assert ec properties and make sure they are correct.
        assertEquals(2, ec.kv.size());
        assertEquals("y", ec.kv.get("x"));
        assertEquals("Scooby Doo", ec.kv.get("name"));
        assertEquals("x=y\nname=Scooby Doo\n", ec.fmsg.getMsg());
        assertEquals("1", ec.fmsg.getId());
        assertEquals("x=y\nname=Scooby Doo\n", ec.str);
    }

    // TODO put this back in with a real test.
    // The system should hand the bad json to the consumer, and let the consumer mark the message as invalid to avoid redelivery of a bad message.
    // @Test(expected=RuntimeException.class)
    public void injectBadJson() {
        Consumer test = new Consumer(ExampleJsonConsumer.class, null, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        ForkliftMessage msg = new ForkliftMessage();
        msg.setId("1");
        msg.setMsg("x=y");

        test.inject(msg, ec);
    }

    @Test
    public void injectEmptyJson() {
        Consumer test = new Consumer(ExampleJsonConsumer.class, forklift, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        ForkliftMessage msg = new ForkliftMessage();
        msg.setId("1");
        msg.setMsg("{}");

        test.inject(msg, ec);
        assertNotNull(ec.msg);
        assertNull(ec.msg.ideas);
        assertNull(ec.msg.name);
        assertNull(ec.msg.url);
    }

    @Test
    public void injectJson() {
        Consumer test = new Consumer(ExampleJsonConsumer.class, forklift, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        ForkliftMessage msg = new ForkliftMessage();
        msg.setId("1");
        msg.setMsg("{\"name\":\"Fred Jones\", \"url\":\"http://forklift\", \"ideas\":[\"scanning\", \"verifying\"]}");

        test.inject(msg, ec);
        assertNotNull(ec.msg);
        assertTrue("scanning".equals(ec.msg.ideas[0]));
        assertEquals(2, ec.msg.ideas.length);
        assertEquals("Fred Jones", ec.msg.name);
        assertEquals("http://forklift", ec.msg.url);
        assertNotNull(ec.headers);
        assertNotNull(ec.properties);
        assertEquals(0, ec.headers.size());
        assertEquals(0, ec.properties.size());
        assertNull(ec.cid);
        assertEquals(ec.producer, "replace");
        assertEquals("default", ec.strval);
    }

    @Test
    public void testHeadersAndProperties() {

        Consumer test = new Consumer(ExampleJsonConsumer.class, forklift, this.getClass().getClassLoader());
        ExampleJsonConsumer ec = new ExampleJsonConsumer();
        ForkliftMessage msg = new ForkliftMessage();
        msg.setId("1");
        msg.setMsg("{}");

        Map<Header, Object> headers = new HashMap<>();
        headers.put(Header.DeliveryCount, "3");
        headers.put(Header.Producer, "testing");
        headers.put(Header.Priority, "1");
        headers.put(Header.CorrelationId, "abcd");
        msg.setHeaders(headers);

        Map<String, String> properties = new HashMap<>();
        properties.put("mystrval", "blah");
        msg.setProperties(properties);

        test.inject(msg, ec);
        assertEquals(4, ec.headers.size());
        assertEquals(1, ec.properties.size());
        assertEquals("blah", ec.mystrval);
        assertEquals("blah", ec.strval);
        assertEquals(ec.cid, "abcd");
        assertEquals(ec.producer, "testing");
    }

    // Class doesn't have queue or topic should throw IllegalArgException
    public class BadConsumer {
    }

    @Queue("q")
    @Topic("a")
    public class DoubleConsumer {

    }

    @Queue("abc")
    public class QueueConsumer {

    }

    @Topic("xyz")
    public class TopicConsumer {

    }

    @Queue("abc")
    public class ExampleConsumer {
        @Message
        ForkliftMessage fmsg;

        @Message
        Map<String, String> kv;

        @Message
        String str;
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

    public static class ExpectedMsg {
        public String name;
        public String url;
        public String[] ideas;

        public ExpectedMsg() {
        }
    }

}
