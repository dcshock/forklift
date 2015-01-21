package forklift.consumer;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.springframework.util.Assert;

import forklift.ForkliftTest;
import forklift.concurrent.Callback;
import forklift.connectors.ForkliftConnectorI;
import forklift.connectors.ForkliftMessage;
import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.decorators.Queue;
import forklift.exception.StartupException;
import forklift.spring.ContextManager;

@Queue("test")
public class ListenerTest extends ForkliftTest {
    /*
     * Have the forklift message injected into multiple different scopes to
     * test injection accessible settings.
     */
    @Message public ForkliftMessage publicMsg;
    @Message private ForkliftMessage privateMsg;
    @Message protected ForkliftMessage protectedMsg;
    @Message ForkliftMessage packageMsg;
    private static AtomicBoolean allNotNull = new AtomicBoolean(false);

    // Store the number of times the onMessage handler was called.
    private static AtomicInteger called = new AtomicInteger(0);

    @OnMessage
    public void onMessage() {
        allNotNull.set(
            publicMsg != null &&
            privateMsg != null &&
            protectedMsg != null &&
            packageMsg != null);
        called.incrementAndGet();
    }

    @Test
    public void test() throws StartupException {
        final MockConnector connector = (MockConnector) ContextManager.getContext().getBean(ForkliftConnectorI.class);
        connector.addMsg();

        final Set<Class<?>> clazzes = new HashSet<Class<?>>();
        clazzes.add(getClass());

        final Consumer c = new Consumer(clazzes);

        // Shutdown the consumer after all the messages have been processed.
        c.getListener(getClass()).setOutOfMessages((listener) -> {
            listener.shutdown();

            Assert.isTrue(allNotNull.get());
            Assert.isTrue(called.get() == 1, "called was not == 1");
        });

        // Start the consumer.
        c.start();
    }
}
