package forklift.producers;

import forklift.connectors.ForkliftMessage;
import forklift.message.Header;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

public class ForkliftSyncProducer<T> implements ForkliftSyncProducerI<T> {
    private ForkliftProducerI producer;
    private ForkliftResultResolver<T> resolver;
    private URI uri;

    public ForkliftSyncProducer(ForkliftProducerI producer, ForkliftResultResolver<T> resolver, String uri) {
        try {
            this.uri = new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        this.producer = producer;
        try {
            this.producer.setProperties(new HashMap<>());
            this.producer.getProperties().put("@ResponseUri", this.uri.toString());

            this.resolver = resolver;
        } catch (ProducerException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Future<T> send(String message)
      throws ProducerException {
        return resolver.register(new ResultFuture<T>(producer.send(message)));
    }

    @Override
    public Future<T> send(ForkliftMessage message)
      throws ProducerException {
        return resolver.register(new ResultFuture<T>(producer.send(message)));
    }

    @Override
    public Future<T> send(Object message)
      throws ProducerException {
        return resolver.register(new ResultFuture<T>(producer.send(message)));
    }

    @Override
    public Future<T> send(Map<String, String> message)
      throws ProducerException {
        return resolver.register(new ResultFuture<T>(producer.send(message)));
    }

    @Override
    public Future<T> send(Map<Header, Object> headers,
                          Map<String, Object> properties,
                          ForkliftMessage message)
      throws ProducerException {
        return resolver.register(new ResultFuture<T>(producer.send(message)));
    }

    @Override
    public Map<Header, Object> getHeaders()
      throws ProducerException {
        return producer.getHeaders();
    }

    @Override
    public void setHeaders(Map<Header, Object> headers)
      throws ProducerException {
        producer.setHeaders(headers);
    }

    @Override
    public Map<String, Object> getProperties()
      throws ProducerException {
        return producer.getProperties();
    }

    @Override
    public void setProperties(Map<String , Object> properties)
      throws ProducerException {
        producer.setProperties(properties);
    }

    @Override
    public void close() {
      try {
        producer.close();
      } catch (IOException ignored) {
      }
    }
}
