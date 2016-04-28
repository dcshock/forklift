package forklift.producers;

import forklift.connectors.ForkliftMessage;
import forklift.message.Header;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.Future;

public interface ForkliftSyncProducerI<T> extends Closeable {
    Future<T> send(String message) throws ProducerException;
    Future<T> send(ForkliftMessage message) throws ProducerException;
    Future<T> send(Object message) throws ProducerException;
    Future<T> send(Map<String, String> message) throws ProducerException;
    Future<T> send(Map<Header, Object> headers,
                Map<String, Object> properties,
                ForkliftMessage message) throws ProducerException;
    Map<Header, Object> getHeaders() throws ProducerException;
    void setHeaders(Map<Header, Object> headers) throws ProducerException;
    Map<String, Object> getProperties() throws ProducerException;
    void setProperties(Map<String , Object> properties) throws ProducerException;
}