package forklift.replay;

import forklift.consumer.ProcessStep;

import java.util.List;
import java.util.Map;

public class ReplayMsg {
    String messageId;
    String queue;
    String topic;
    ProcessStep step;
    String text;
    Map<forklift.message.Header, Object> headers;
    Map<String, Object> properties;
    List<String> errors;
    String time;

    public String getMessageId() {
        return messageId;
    }

    public String getQueue() {
        return queue;
    }

    public String getTopic() {
        return topic;
    }

    public ProcessStep getStep() {
        return step;
    }

    public String getText() {
        return text;
    }

    public Map<forklift.message.Header, Object> getHeaders() {
        return headers;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public List<String> getErrors() {
        return errors;
    }

    public String getTime() {
        return time;
    }
}