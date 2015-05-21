package forklift.activemq.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import forklift.consumer.ProcessStep;
import forklift.message.ActiveMQHeaders;
import forklift.message.Header;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQMessage;
import org.junit.Test;

import java.util.Date;

public class ActiveMQHeadersTest {
    @Test
    public void setAllHeaders() {
        Date now = new Date();
        ActiveMQMessage msg = new ActiveMQMessage();
        ActiveMQHeaders.getFunctions().get(Header.CorrelationId).set(msg, "5");
        ActiveMQHeaders.getFunctions().get(Header.DeliveryCount).set(msg, 3);
        ActiveMQHeaders.getFunctions().get(Header.DeliveryMode).set(msg, 1);
        ActiveMQHeaders.getFunctions().get(Header.Expiration).set(msg, now.getTime());
        ActiveMQHeaders.getFunctions().get(Header.GroupId).set(msg, "2");
        ActiveMQHeaders.getFunctions().get(Header.GroupSeq).set(msg, 10);
        ActiveMQHeaders.getFunctions().get(Header.PreviousDestination).set(msg, "queue://xyz");
        ActiveMQHeaders.getFunctions().get(Header.Priority).set(msg, 9);
        ActiveMQHeaders.getFunctions().get(Header.Producer).set(msg, "Forklift");
        ActiveMQHeaders.getFunctions().get(Header.ReplyTo).set(msg, "queue://F2");
        ActiveMQHeaders.getFunctions().get(Header.Result).set(msg, ProcessStep.Complete.name());
        ActiveMQHeaders.getFunctions().get(Header.ResultDetail).set(msg, "FORKLIFT MSG SUCCESSFUL");
        ActiveMQHeaders.getFunctions().get(Header.RetryCount).set(msg, 10);
        ActiveMQHeaders.getFunctions().get(Header.Timestamp).set(msg, now.getTime());
        ActiveMQHeaders.getFunctions().get(Header.Type).set(msg, "JSON");

        assertEquals("5", ActiveMQHeaders.getFunctions().get(Header.CorrelationId).get(msg));
        assertEquals(3, ActiveMQHeaders.getFunctions().get(Header.DeliveryCount).get(msg));
        assertEquals(1, ActiveMQHeaders.getFunctions().get(Header.DeliveryMode).get(msg));
        assertEquals(now.getTime(), ActiveMQHeaders.getFunctions().get(Header.Expiration).get(msg));
        assertEquals("2", ActiveMQHeaders.getFunctions().get(Header.GroupId).get(msg));
        assertEquals(10, ActiveMQHeaders.getFunctions().get(Header.GroupSeq).get(msg));
        assertEquals("queue://xyz", ActiveMQHeaders.getFunctions().get(Header.PreviousDestination).get(msg));
        assertEquals(9, ActiveMQHeaders.getFunctions().get(Header.Priority).get(msg));
        assertEquals("Forklift", ActiveMQHeaders.getFunctions().get(Header.Producer).get(msg));
        assertEquals("queue://F2", ActiveMQHeaders.getFunctions().get(Header.ReplyTo).get(msg));
        assertEquals(ProcessStep.Complete.name(), ActiveMQHeaders.getFunctions().get(Header.Result).get(msg));
        assertEquals("FORKLIFT MSG SUCCESSFUL", ActiveMQHeaders.getFunctions().get(Header.ResultDetail).get(msg));
        assertEquals(10, ActiveMQHeaders.getFunctions().get(Header.RetryCount).get(msg));
        assertEquals(now.getTime(), ActiveMQHeaders.getFunctions().get(Header.Timestamp).get(msg));
        assertEquals("JSON", ActiveMQHeaders.getFunctions().get(Header.Type).get(msg));
    }

    @Test
    public void setNullHeaders() {
        ActiveMQMessage msg = new ActiveMQMessage();
        ActiveMQHeaders.getFunctions().get(Header.CorrelationId).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.DeliveryCount).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.DeliveryMode).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.Expiration).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.GroupId).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.GroupSeq).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.PreviousDestination).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.Priority).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.Producer).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.ReplyTo).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.Result).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.ResultDetail).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.RetryCount).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.Timestamp).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.Type).set(msg, null);

        assertNull(ActiveMQHeaders.getFunctions().get(Header.CorrelationId).get(msg));
        assertEquals(0, ActiveMQHeaders.getFunctions().get(Header.DeliveryCount).get(msg));
        assertEquals(1, ActiveMQHeaders.getFunctions().get(Header.DeliveryMode).get(msg));
        assertEquals(0L, ActiveMQHeaders.getFunctions().get(Header.Expiration).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.GroupId).get(msg));
        assertEquals(0, ActiveMQHeaders.getFunctions().get(Header.GroupSeq).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.PreviousDestination).get(msg));
        assertEquals(0, ActiveMQHeaders.getFunctions().get(Header.Priority).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.Producer).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.ReplyTo).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.Result).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.ResultDetail).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.RetryCount).get(msg));
        assertEquals(0L, ActiveMQHeaders.getFunctions().get(Header.Timestamp).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.Type).get(msg));
    }

    @Test
    public void setBadHeaders() {
        ActiveMQMessage msg = new ActiveMQMessage();
        ActiveMQHeaders.getFunctions().get(Header.CorrelationId).set(msg, 1.0);
        ActiveMQHeaders.getFunctions().get(Header.DeliveryCount).set(msg, "abc");
        ActiveMQHeaders.getFunctions().get(Header.DeliveryMode).set(msg, "1");
        ActiveMQHeaders.getFunctions().get(Header.Expiration).set(msg, "never");
        ActiveMQHeaders.getFunctions().get(Header.GroupId).set(msg, new Long(1));
        ActiveMQHeaders.getFunctions().get(Header.GroupSeq).set(msg, "10");
        ActiveMQHeaders.getFunctions().get(Header.PreviousDestination).set(msg, new ActiveMQDestination() {
            @Override
            public byte getDataStructureType() {
                return 0;
            }
            @Override
            protected String getQualifiedPrefix() {
                return null;
            }
            @Override
            public byte getDestinationType() {
                return 0;
            }
            public String toString() {
                return "queue://abc";
            }
        });
        ActiveMQHeaders.getFunctions().get(Header.Priority).set(msg, "high");
        ActiveMQHeaders.getFunctions().get(Header.Producer).set(msg, Header.Producer);
        ActiveMQHeaders.getFunctions().get(Header.ReplyTo).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.Result).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.ResultDetail).set(msg, null);
        ActiveMQHeaders.getFunctions().get(Header.RetryCount).set(msg, "5 - Five");
        ActiveMQHeaders.getFunctions().get(Header.Timestamp).set(msg, new Date());
        ActiveMQHeaders.getFunctions().get(Header.Type).set(msg, "K/V");

        assertEquals("1.0", ActiveMQHeaders.getFunctions().get(Header.CorrelationId).get(msg));
        assertEquals(0, ActiveMQHeaders.getFunctions().get(Header.DeliveryCount).get(msg));
        assertEquals(1, ActiveMQHeaders.getFunctions().get(Header.DeliveryMode).get(msg));
        assertEquals(0L, ActiveMQHeaders.getFunctions().get(Header.Expiration).get(msg));
        assertEquals("1", ActiveMQHeaders.getFunctions().get(Header.GroupId).get(msg));
        assertEquals(10, ActiveMQHeaders.getFunctions().get(Header.GroupSeq).get(msg));
        assertEquals("queue://abc", ActiveMQHeaders.getFunctions().get(Header.PreviousDestination).get(msg));
        assertEquals(0, ActiveMQHeaders.getFunctions().get(Header.Priority).get(msg));
        assertEquals(Header.Producer.toString(), ActiveMQHeaders.getFunctions().get(Header.Producer).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.ReplyTo).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.Result).get(msg));
        assertNull(ActiveMQHeaders.getFunctions().get(Header.ResultDetail).get(msg));
        assertEquals(0, ActiveMQHeaders.getFunctions().get(Header.RetryCount).get(msg));
        assertEquals(0L, ActiveMQHeaders.getFunctions().get(Header.Timestamp).get(msg));
        assertEquals("K/V", ActiveMQHeaders.getFunctions().get(Header.Type).get(msg));
    }
}
