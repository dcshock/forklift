package forklift.message;

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
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                jmsg.setJMSCorrelationID(obj.toString());
            }
        });
        setup.put(Header.DeliveryCount, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getRedeliveryCounter();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                int count = 0;
                if (obj instanceof Integer) {
                    count = ((Integer)obj).intValue();
                } else {
                    try {
                        count = Integer.parseInt(obj.toString());
                    } catch (NumberFormatException ignored) {}
                }
                jmsg.setRedeliveryCounter(count);
            }
        });
        setup.put(Header.DeliveryMode, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSDeliveryMode();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                int mode = 0;
                if (obj instanceof Integer) {
                    mode = ((Integer)obj).intValue();
                } else {
                    try {
                        mode = Integer.parseInt(obj.toString());
                    } catch (NumberFormatException ignored) {}
                }
                jmsg.setJMSDeliveryMode(mode);
            }
        });
        setup.put(Header.Expiration, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSExpiration();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                long expiration = 0;
                if (obj instanceof Long) {
                    expiration = ((Long)obj).longValue();
                } else {
                    try {
                        expiration = Long.parseLong(obj.toString());
                    } catch (NumberFormatException ignored) {}
                }
                jmsg.setJMSExpiration(expiration);
            }
        });
        setup.put(Header.GroupId, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getGroupID();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                jmsg.setGroupID(obj.toString());
            }
        });
        setup.put(Header.GroupSeq, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getGroupSequence();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                int groupSequence = 0;
                if (obj instanceof Integer) {
                    groupSequence = ((Integer)obj).intValue();
                } else {
                    try {
                        groupSequence = Integer.parseInt(obj.toString());
                    } catch (NumberFormatException ignored) {}
                }
                jmsg.setGroupSequence(groupSequence);
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
            public void set(ActiveMQMessage jmsg, Object fmsg) {
                if (fmsg == null)
                    return;
                try {
                    jmsg.setProperty("JMSPPreviousDestination", fmsg.toString());
                } catch (IOException ignored) { }
            }
        });
        setup.put(Header.Priority, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSPriority();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                int priority = 0;
                if (obj instanceof Integer) {
                    priority = ((Integer)obj).intValue();
                } else {
                    try {
                        priority = Integer.parseInt(obj.toString());
                    } catch (NumberFormatException ignored) {}
                }
                jmsg.setJMSPriority(priority);
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
            public void set(ActiveMQMessage jmsg, Object fmsg) {
                if (fmsg == null)
                    return;
                try {
                    jmsg.setProperty("JMSPProducer", fmsg.toString());
                } catch (IOException ignored) { }
            }
        });
        // We will not use the ActiveMQ ReplyTo instead use a String property so we
        // can use a standard URI string.
        setup.put(Header.ReplyTo, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                try {
                    return m.getStringProperty("JMSPReplyTo");
                } catch (JMSException e) {
                    return null;
                }
            }
            public void set(ActiveMQMessage jmsg, Object fmsg) {
                if (fmsg == null)
                    return;
                try {
                    jmsg.setProperty("JMSPReplyTo", fmsg.toString());
                } catch (IOException ignored) { }
            }
        });
        setup.put(Header.Result, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                try {
                    return m.getStringProperty("JMSPResult");
                } catch (JMSException e) {
                    return null;
                }
            }
            public void set(ActiveMQMessage jmsg, Object fmsg) {
                if (fmsg == null)
                    return;
                try {
                    jmsg.setProperty("JMSPResult", fmsg.toString());
                } catch (IOException ignored) { }
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
            public void set(ActiveMQMessage jmsg, Object fmsg) {
                if (fmsg == null)
                    return;
                try {
                    jmsg.setProperty("JMSPResultDetail", fmsg.toString());
                } catch (IOException ignored) { }
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
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                int value = 0;
                if (obj instanceof Integer) {
                    value = ((Integer)obj).intValue();
                } else {
                    try {
                        value = Integer.parseInt(obj.toString());
                    } catch (NumberFormatException ignored) {}
                }
                try {
                    jmsg.setIntProperty("JMSPRetryCount", value);
                } catch (JMSException ignored) { }
            }
        });
        setup.put(Header.Timestamp, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSTimestamp();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                long timestamp = 0;
                if (obj instanceof Long) {
                    timestamp = ((Long)obj).longValue();
                } else {
                    try {
                        timestamp = Long.parseLong(obj.toString());
                    } catch (NumberFormatException ignored) {}
                }
                jmsg.setJMSTimestamp(timestamp);
            }
        });
        setup.put(Header.Type, new JMSMethodCallI() {
            public Object get(ActiveMQMessage m) {
                if (m == null)
                    return null;
                return m.getJMSType();
            }
            public void set(ActiveMQMessage jmsg, Object obj) {
                if (obj == null)
                    return;
                jmsg.setJMSType(obj.toString());
            }
        });

        functions = Collections.unmodifiableMap(setup);
    }

    public static Map<Header, JMSMethodCallI> getFunctions() {
        return functions;
    }

    public interface JMSMethodCallI {
        Object get(ActiveMQMessage m);
        void set(ActiveMQMessage jmsg, Object fmsg);
    }
}
