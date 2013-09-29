package forklift.jms;

import javax.jms.Connection;

public interface ConnectionManagerI {
    void start();
    void stop();
    Connection getConnection();
}
