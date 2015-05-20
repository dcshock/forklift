package forklift;

import forklift.message.Header;

import org.apache.activemq.command.ActiveMQMessage;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.jms.JMSException;

public class ActiveMQHeaders {
    // Build headers
    private final static Map<Header, JMSMethodCallI> functions;

    static {
        Map<Header, JMSMethodCallI> setup = new HashMap<>();
        setup.put(Header.CorrelationId, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSCorrelationID();
            }
        });
        setup.put(Header.DeliveryCount, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getRedeliveryCounter();
            }
        });
        setup.put(Header.DeliveryMode, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSDeliveryMode();
            }

        });
        setup.put(Header.Expiration, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSExpiration();
            }

        });
        setup.put(Header.GroupID, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getGroupID();
            }
        });
        setup.put(Header.GroupSeq, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getGroupSequence();
            }
        });
        setup.put(Header.PreviousDestination, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                try {
                    return m.getStringProperty("JMSPPreviousDestination");
                } catch (JMSException e) {
                    return null;
                }
            }
        });
        setup.put(Header.Priority, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSPriority();
            }
        });
        setup.put(Header.Producer, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                try {
                    return m.getStringProperty("JMSPProducer");
                } catch (JMSException e) {
                    return null;
                }
            }
        });
        setup.put(Header.ReplyTo, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                if (m.getReplyTo() == null)
                    return null;
                return m.getReplyTo().toString();
            }
        });
        setup.put(Header.Result, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                try {
                    return m.getStringProperty("JMSPProducer");
                } catch (JMSException e) {
                    return null;
                }
            }
        });
        setup.put(Header.ResultDetail, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                try {
                    return m.getStringProperty("JMSPResultDetail");
                } catch (JMSException e) {
                    return null;
                }
            }
        });
        setup.put(Header.RetryCount, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                try {
                    if (m.getProperty("JMSPRetryCount") == null)
                        return null;
                    return m.getIntProperty("JMSPRetryCount");
                } catch (IOException | JMSException | NumberFormatException e) {
                    return null;
                }
            }
        });
        setup.put(Header.Timestamp, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSTimestamp();
            }
        });
        setup.put(Header.Type, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSType();
            }
        });

        functions = Collections.unmodifiableMap(setup);
    }

    public static Map<Header, JMSMethodCallI> getFunctions() {
        return functions;
    }

    public interface JMSMethodCallI {
        Object get(ActiveMQMessage m);
    }
}
