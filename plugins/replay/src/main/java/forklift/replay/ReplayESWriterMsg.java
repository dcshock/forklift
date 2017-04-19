package forklift.replay;

import java.util.Map;

public class ReplayESWriterMsg {
    private String id;
    private Map<String, String> fields;
    private long version = System.currentTimeMillis();

    public ReplayESWriterMsg() {

    }

    public ReplayESWriterMsg(String id, Map<String, String> fields) {
        this.id = id;
        this.fields = fields;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }
}
