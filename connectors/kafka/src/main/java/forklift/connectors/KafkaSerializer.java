package forklift.connectors;

import forklift.source.SourceI;
import forklift.source.sources.GroupedTopicSource;
import forklift.source.sources.RoleInputSource;
import forklift.source.sources.QueueSource;
import forklift.source.sources.TopicSource;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class KafkaSerializer implements ForkliftSerializer {
    private KafkaConnector connector;
    private Serializer serializer;
    private Deserializer deserializer;

    public KafkaSerializer(KafkaConnector connector, Serializer serializer, Deserializer deserializer) {
        this.connector = connector;
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public byte[] serializeForSource(SourceI source, Object o) {
        final String topicName = source
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .apply(RoleInputSource.class, roleSource -> connector.mapRoleInputSource(roleSource).getName())
            .elseUnsupportedError();

        return serializer.serialize(topicName, o);
    }

    @Override
    public <T> T deserializeForSource(SourceI source, byte[] bytes) {
        final String topicName = source
            .apply(QueueSource.class, queue -> queue.getName())
            .apply(TopicSource.class, topic -> topic.getName())
            .apply(GroupedTopicSource.class, topic -> topic.getName())
            .apply(RoleInputSource.class, roleSource -> connector.mapRoleInputSource(roleSource).getName())
            .elseUnsupportedError();

        return (T) deserializer.deserialize(topicName, bytes);
    }
}
