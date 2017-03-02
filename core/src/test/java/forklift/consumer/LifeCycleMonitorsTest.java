package forklift.consumer;

import static org.junit.Assert.assertTrue;

import forklift.TestMsg;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.lifecycle.BadAuditor;
import forklift.consumer.lifecycle.TestAuditor;
import forklift.consumer.lifecycle.TestAuditor2;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.jms.Message;

public class LifeCycleMonitorsTest {
    private static final Logger log = LoggerFactory.getLogger(LifeCycleMonitorsTest.class);

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
                    LifeCycleMonitors.call(ps, new MessageRunnable(null, new ForkliftMessage(jmsMsg), null, null, null, null, null, null, Collections.emptyList()));
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

    // Register a listener that blows up during instantiation. Make sure it doesn't crash
    // the system.
    @Test
    public void badListener() {
        LifeCycleMonitors.register(BadAuditor.class);

        log.debug("The following generates an exception. This is expected.");
        // Now the validate listener should log out an error but should stop processing from happening.
        Message jmsMsg = new TestMsg("1");
        LifeCycleMonitors.call(ProcessStep.Validating, new MessageRunnable(null, new ForkliftMessage(jmsMsg), null, null, null, null, null, null, Collections.emptyList()));
        assertTrue("Make sure the exception was eaten and just logged.", true);
    }
}
