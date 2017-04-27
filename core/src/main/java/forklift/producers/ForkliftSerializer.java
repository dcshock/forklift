package forklift.producers;

import forklift.source.SourceI;

public interface ForkliftSerializer {
    /**
     * Serialize the given object to bytes.
     *
     * @param source the source for which the serialized bytes are destined
     * @param object the object to serialize
     * @return the byte serialization of the given object
     */
    public byte[] serializeForSource(SourceI source, Object object);
}
