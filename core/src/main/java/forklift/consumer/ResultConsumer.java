package forklift.consumer;

import forklift.connectors.ForkliftMessage;
import forklift.decorators.Message;
import forklift.decorators.OnMessage;
import forklift.decorators.Topic;
import forklift.message.Header;
import forklift.producers.ForkliftResultResolver;


@Topic("sync.result")
public class ResultConsumer<T> {
    @SuppressWarnings("rawtypes")
    private static ForkliftResultResolver resolver;

    @Message private ForkliftMessage msg;
    @Message private T t;

    @SuppressWarnings("unchecked")
    @OnMessage
    public void onMessage() {
        System.out.println("ON MESSAGE: " + msg.getMsg());
        resolver.resolve("" + msg.getHeaders().get(Header.CorrelationId), t);
    }

    @SuppressWarnings("rawtypes")
    public static void setResolver(ForkliftResultResolver resolver) {
        ResultConsumer.resolver = resolver;
    }
}
