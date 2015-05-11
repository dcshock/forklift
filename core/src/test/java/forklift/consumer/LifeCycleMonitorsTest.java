package forklift.consumer;

import forklift.consumer.lifecycle.TestAuditor;
import forklift.consumer.lifecycle.TestAuditor2;

import org.junit.Test;

import java.util.Enumeration;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

public class LifeCycleMonitorsTest {

    AtomicBoolean registered = new AtomicBoolean(false);
    AtomicInteger threads = new AtomicInteger(0);
    Lock lock = new ReentrantLock();

    // Run a bunch of threads at the same time registering and deregistering while
    // calls run amuck. If any of the registrations happen while a call is taking
    // place, LifeCycleMonitors should throw an exception, which would blow up this
    // test.
    @Test
    public void test() throws InterruptedException {
        LifeCycleMonitors.register(TestAuditor.class);

        final Runnable calls = new Runnable() {
            @Override
            public void run() {
                threads.getAndIncrement();
                for (int i = 0; i < 40; i++) {
                    Message jmsMsg = new TestMsg("" + i);
                    int next = new Random().nextInt(ProcessStep.values().length);
                    ProcessStep ps = ProcessStep.values()[next];
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    LifeCycleMonitors.call(ps, new MessageRunnable(jmsMsg, null, null, null, null));
                }
                threads.getAndDecrement();
            }
        };

        new Thread(calls).start();
        new Thread(calls).start();
        new Thread(calls).start();
        new Thread(calls).start();
        new Thread(calls).start();

        // Now try and register to make sure it blocks
        Runnable reg = new Runnable() {
            @Override
            public void run() {
                lock.lock();
                threads.getAndIncrement();
                if (! registered.getAndSet(true)) {
                    LifeCycleMonitors.register(TestAuditor2.class);
                    LifeCycleMonitors.deregister(TestAuditor2.class);
                    registered.getAndSet(false);
                }
                threads.getAndDecrement();
                lock.unlock();
            }
        };

        Thread.sleep(100);
        new Thread(reg).start();
        new Thread(reg).start();
        Thread.sleep(1000);
        new Thread(reg).start();
        new Thread(reg).start();
        new Thread(reg).start();

        Runnable unregOrig = new Runnable() {
            @Override
            public void run() {
                threads.getAndIncrement();
                LifeCycleMonitors.deregister(TestAuditor.class);
                threads.getAndDecrement();
            }
        };
        Thread.sleep(6000);
        new Thread(unregOrig).start();

        synchronized(threads) {
            while (threads.get() > 0) {
                threads.wait(1000);
            }
        }
    }

    public class TestMsg implements Message {
        private String corrId;

        TestMsg(String id) {
            this.corrId = id;
        }

        @Override
        public void acknowledge() throws JMSException {
            System.out.println("ack id " + corrId);
        }
        @Override
        public void clearBody() throws JMSException { }
        @Override
        public void clearProperties() throws JMSException { }
        @Override
        public boolean getBooleanProperty(String arg0) throws JMSException { return false; }
        @Override
        public byte getByteProperty(String arg0) throws JMSException { return 0; }
        @Override
        public double getDoubleProperty(String arg0) throws JMSException {	return 0; }
        @Override
        public float getFloatProperty(String arg0) throws JMSException { return 0; }
        @Override
        public int getIntProperty(String arg0) throws JMSException { return 0; }
        @Override
        public String getJMSCorrelationID() throws JMSException { return corrId; }
        @Override
        public byte[] getJMSCorrelationIDAsBytes() throws JMSException { return null; }
        @Override
        public int getJMSDeliveryMode() throws JMSException { return 0; }
        @Override
        public Destination getJMSDestination() throws JMSException { return null; }
        @Override
        public long getJMSExpiration() throws JMSException { return 0; }
        @Override
        public String getJMSMessageID() throws JMSException { return null; }
        @Override
        public int getJMSPriority() throws JMSException { return 0; }
        @Override
        public boolean getJMSRedelivered() throws JMSException { return false; }
        @Override
        public Destination getJMSReplyTo() throws JMSException { return null; }
        @Override
        public long getJMSTimestamp() throws JMSException { return 0; }
        @Override
        public String getJMSType() throws JMSException { return null; }
        @Override
        public long getLongProperty(String arg0) throws JMSException { return 0; }
        @Override
        public Object getObjectProperty(String arg0) throws JMSException { return null; }
        @Override
        public Enumeration getPropertyNames() throws JMSException { return null; }
        @Override
        public short getShortProperty(String arg0) throws JMSException { return 0; }
        @Override
        public String getStringProperty(String arg0) throws JMSException { return null; }
        @Override
        public boolean propertyExists(String arg0) throws JMSException { return false; }
        @Override
        public void setBooleanProperty(String arg0, boolean arg1) throws JMSException { }
        @Override
        public void setByteProperty(String arg0, byte arg1) throws JMSException { }
        @Override
        public void setDoubleProperty(String arg0, double arg1) throws JMSException { }
        @Override
        public void setFloatProperty(String arg0, float arg1) throws JMSException { }
        @Override
        public void setIntProperty(String arg0, int arg1) throws JMSException { }
        @Override
        public void setJMSCorrelationID(String arg0) throws JMSException {
            corrId = arg0;
        }
        @Override
        public void setJMSCorrelationIDAsBytes(byte[] arg0) throws JMSException { }
        @Override
        public void setJMSDeliveryMode(int arg0) throws JMSException { }
        @Override
        public void setJMSDestination(Destination arg0) throws JMSException { }
        @Override
        public void setJMSExpiration(long arg0) throws JMSException { }
        @Override
        public void setJMSMessageID(String arg0) throws JMSException { }
        @Override
        public void setJMSPriority(int arg0) throws JMSException { }
        @Override
        public void setJMSRedelivered(boolean arg0) throws JMSException { }
        @Override
        public void setJMSReplyTo(Destination arg0) throws JMSException { }
        @Override
        public void setJMSTimestamp(long arg0) throws JMSException { }
        @Override
        public void setJMSType(String arg0) throws JMSException { }
        @Override
        public void setLongProperty(String arg0, long arg1) throws JMSException { }
        @Override
        public void setObjectProperty(String arg0, Object arg1) throws JMSException { }
        @Override
        public void setShortProperty(String arg0, short arg1) throws JMSException { }
        @Override
        public void setStringProperty(String arg0, String arg1) throws JMSException { }
        @Override
        public String toString() {
            return corrId;
        }
    }
}
