//package forklift.consumer;
//
//import java.io.Closeable;
//import java.util.List;
//
//public class MessageHandlerWrapper {
//
//    private final Object messageHandler;
//    private final List<Closeable> closeable;
//
//    public MessageHandlerWrapper(Object messageHandler, List<Closeable> closeable) {
//        this.messageHandler = messageHandler;
//        this.closeable = closeable;
//    }
//
//    public Object getMessageHandler() {
//        return messageHandler;
//    }
//
//    public List<Closeable> getCloseable() {
//        return closeable;
//    }
//}
