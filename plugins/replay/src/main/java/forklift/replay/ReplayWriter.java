package forklift.replay;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.io.Files;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;

public class ReplayWriter extends ReplayStoreThread<ReplayMsg> {
    private ObjectMapper mapper;
    private BufferedWriter writer;

    public ReplayWriter(File file) throws FileNotFoundException {
        super();
        this.mapper = new ObjectMapper().registerModule(new JavaTimeModule())
                                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                                        .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        this.writer = Files.newWriter(file, Charset.forName("UTF-8"));
    }

    @Override
    protected void poll(ReplayMsg t) {
        try {
            writer.write(mapper.writeValueAsString(t) + "\n");
        } catch (IOException e) {
            log.error("", e);
        }
    }

    @Override
    protected void emptyPoll() {
        try {
            writer.flush();
        } catch (IOException e) {
            log.error("", e);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
        writer.flush();
        writer.close();
    }
}
