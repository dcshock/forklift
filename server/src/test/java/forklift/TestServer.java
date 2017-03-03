package forklift;

import static org.junit.Assert.assertTrue;
import com.mashape.unirest.http.options.Options;
import org.junit.Test;
import java.util.concurrent.TimeUnit;

/**
 * Created by afrieze on 3/1/17.
 */
public class TestServer {

    @Test
    public void testConsumers() throws InterruptedException{
        ForkliftOpts options = new ForkliftOpts();
        String ip = "localhost";
        options.setBrokerUrl("consul.kafka.schema-registry");
        options.setApplicationName("app10");
        options.setConsulHost(ip);
        ForkliftServer server = new ForkliftServer(options);
        try {
            server.startServer(10000, TimeUnit.SECONDS);
            // Forklift closes Unirest...we need to reopen it with a call to Options.refresh()
            Options.refresh();
        } catch (InterruptedException e) {
        }
        if (server.getServerState() == ServerState.RUNNING) {
            server.registerDeployment(TestStringConsumer.class, TestObjectConsumer.class, TestMapConsumer.class, TestPersonConsumer.class);
        }
        else {
            try {
                server.stopServer(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
            }
            System.exit(-1);
        }
        Thread.sleep(130000);
        server.stopServer(5, TimeUnit.SECONDS);
        assertTrue(true);
    }
}
