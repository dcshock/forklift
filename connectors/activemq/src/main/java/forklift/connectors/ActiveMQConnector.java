package forklift.connectors;

import forklift.consumer.ActiveMQMessageConsumer;
import forklift.consumer.ForkliftConsumerI;
import forklift.consumer.wrapper.RoleInputConsumerWrapper;
import forklift.producers.ActiveMQProducer;
import forklift.producers.ForkliftProducerI;
import forklift.source.ActionSource;
import forklift.source.LogicalSource;
import forklift.source.SourceI;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.RoleInputSource;
import forklift.source.sources.TopicSource;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

public class ActiveMQConnector implements ForkliftConnectorI {
    private Logger log = LoggerFactory.getLogger(ActiveMQConnector.class);

    private ActiveMQConnectionFactory factory;
    private ActiveMQConnection conn;
    private String brokerUrl;

    public ActiveMQConnector() {

    }

    public ActiveMQConnector(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    @Override
    public synchronized void start()
      throws ConnectorException {
        if (brokerUrl == null)
            throw new ConnectorException("brokerUrl wasn't set");

        factory = new ActiveMQConnectionFactory("", "", brokerUrl);
    }

    @Override
    public synchronized void stop()
      throws ConnectorException {
        if (conn != null)
            try {
                conn.close();
            } catch (JMSException e) {
                throw new ConnectorException(e.getMessage());
            }
    }

    public synchronized Connection getConnection()
      throws ConnectorException {
        if (conn == null || !conn.isStarted())
            try {
                conn = (ActiveMQConnection)factory.createConnection();
                conn.start();
            } catch (JMSException e) {
                throw new ConnectorException(e.getMessage());
            }

        if (conn == null)
            throw new ConnectorException("Could not create connection to activemq.");

        return conn;
    }

    public synchronized Session getSession()
      throws ConnectorException {
        try {
            return getConnection().createSession(
                false, ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE);
        } catch (JMSException e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    private ForkliftConsumerI getQueue(String name)
      throws ConnectorException {
        final Session s = getSession();
        try {
            return new ActiveMQMessageConsumer(s.createConsumer(s.createQueue(name)), s);
        } catch (JMSException e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    private ForkliftConsumerI getTopic(String name)
      throws ConnectorException {
        final Session s = getSession();
        try {
            return new ActiveMQMessageConsumer(s.createConsumer(s.createTopic(name)), s);
        } catch (JMSException e) {
            throw new ConnectorException(e.getMessage());
        }
    }

    @Override
    public ForkliftConsumerI getConsumerForSource(SourceI source) throws ConnectorException {
        return source
            .apply(QueueSource.class, queue ->   getQueue(queue.getName()))
            .apply(TopicSource.class, topic -> getTopic(topic.getName()))
            .apply(GroupedTopicSource.class, topic -> getGroupedTopic(topic))
            .apply(RoleInputSource.class, roleSource -> {
                final ForkliftConsumerI rawConsumer = getConsumerForSource(roleSource.getActionSource(this));
                return new RoleInputConsumerWrapper(rawConsumer);
            })
            .elseUnsupportedError();
    }

    private ForkliftConsumerI getGroupedTopic(GroupedTopicSource source) throws ConnectorException {
        if (!source.groupSpecified())
            throw new ConnectorException("No consumer group specified");

        // Sanitize the group and topic names so that group and topic names don't accidentally
        // mess up ActiveMQ's pattern matching (e.g. by making a group name of
        // "test.VirtualTopic.blah" and a topic of "topic", which would cause AMQ to look for a topic
        // named "blah.VirtualTopic.topic" and a group of "test")
        final String groupName = source.getGroup().replaceAll("\\.", "_");
        final String topicName = source.getName();

        return getConsumerForSource(new QueueSource("Consumer." + groupName + ".VirtualTopic." + topicName));
    }

    @Override
    public ForkliftProducerI getQueueProducer(String name) {
        try {
            final Session s = getSession();
            return new ActiveMQProducer(s.createProducer(new ActiveMQQueue(name)), s);
        } catch (JMSException | ConnectorException e) {
            log.error("getQueueProducer, throwing error", e);
            return null;
        }
    }

    @Override
    public ForkliftProducerI getTopicProducer(String name) {
         try {
            final Session s = getSession();
            return new ActiveMQProducer(s.createProducer(new ActiveMQTopic(name)), s);
        } catch (JMSException | ConnectorException e) {
            log.error("getTopicProducer, throwing error", e);
            return null;
        }
    }

    @Override
    public ActionSource mapSource(LogicalSource source) {
        return source
            .apply(RoleInputSource.class, roleSource -> new QueueSource("forklift-role-" + roleSource.getRole()))
            .get();
    }

    @Override
    public boolean supportsOrder() {
        return true;
    }

    @Override
    public boolean supportsResponse() {
        return true;
    }
}
