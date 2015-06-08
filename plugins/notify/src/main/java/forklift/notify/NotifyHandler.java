package forklift.notify;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import forklift.consumer.MessageRunnable;
import forklift.consumer.ProcessStep;
import forklift.decorators.LifeCycle;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;

import java.io.IOException;

public class NotifyHandler {
    private ObjectMapper mapper;

    public NotifyHandler() {
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    @LifeCycle(value=ProcessStep.Pending)
    @LifeCycle(value=ProcessStep.Validating)
    @LifeCycle(value=ProcessStep.Invalid)
    @LifeCycle(value=ProcessStep.Processing)
    @LifeCycle(value=ProcessStep.Complete)
    @LifeCycle(value=ProcessStep.Error)
    public void all(MessageRunnable mr) {
        final Class<?> c = mr.getConsumer().getMsgHandler();

        final NotifyPost notifyPost = c.getAnnotation(NotifyPost.class);
        if (notifyPost != null)
            notifyPost(mr, notifyPost);
    }

    private void notifyPost(MessageRunnable mr, NotifyPost notifyPost) {
        if (!match(mr.getProcessStep(), notifyPost.steps()))
            return;

        try {
            Response resp = Request.Post(notifyPost.url())
                                   .bodyString(mapper.writeValueAsString(""), ContentType.APPLICATION_JSON)
                                   .execute();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean match(ProcessStep a, ProcessStep... b) {
        for (ProcessStep s : b)
            if (a == s)
                return true;

        return false;
    }
}
