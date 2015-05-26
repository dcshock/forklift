package forklift.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.io.Files;
import forklift.connectors.ForkliftMessage;
import forklift.consumer.ProcessStep;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

public class ReplayWriter extends Thread implements Closeable {
    private AtomicBoolean running = new AtomicBoolean(false);
    private BlockingQueue<ReplayMsg> queue = new ArrayBlockingQueue<>(1000);
    private ObjectMapper mapper;
    private BufferedWriter writer;


    public ReplayWriter(File file) throws FileNotFoundException {
        super();
        this.setDaemon(true);
        this.setName("ReplayWriter");
        this.mapper = new ObjectMapper();
        this.mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        this.writer = Files.newWriter(file, Charset.forName("UTF-8"));
    }

    @Override
    public void run() {
        running.set(true);
        try {
            while (running.get()) {
                writer.write(mapper.writeValueAsString(queue.poll(2, TimeUnit.SECONDS)) + "\n");
            }

            // Drain to the file.
            final List<ReplayMsg> msgs = new ArrayList<>();
            queue.drainTo(msgs);
            for (ReplayMsg msg : msgs)
                writer.write(mapper.writeValueAsString(msg) + "\n");
        } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                return;
        }
    }

    public void write(ForkliftMessage msg, ProcessStep step, List<String> errors) {
        try {
            final ReplayMsg replayMsg = new ReplayMsg();
            try {
                replayMsg.messageId = msg.getJmsMsg().getJMSMessageID();
            } catch (JMSException ignored) {
            }
            replayMsg.text = msg.getMsg();
            replayMsg.headers = msg.getHeaders();
            replayMsg.step = step;
            replayMsg.properties = msg.getProperties();
            replayMsg.errors = errors;
            queue.put(replayMsg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        try {
            this.join(20 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        writer.flush();
        writer.close();
    }

    private class ReplayMsg {
        String messageId;
        ProcessStep step;
        String text;
        Map<forklift.message.Header, Object> headers;
        Map<String, Object> properties;
        List<String> errors;

        public String getMessageId() {
            return messageId;
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
    }
}
