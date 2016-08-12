package forklift.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoordinationState {
    private static String version = "1";
    private String name;

    // need to add timeout of some sort to this "known" - when did I know this about you?
    private Map<String, List<String>> known;
    private List<String> groups;

    public CoordinationState () {}

    public CoordinationState(String name) {
        this.name = name;
        this.known = new HashMap<>();
        this.groups = new ArrayList<>();
    }

    public CoordinationState(String name, Map<String, List<String>> known, List<String> groups) {
        this.name = name;
        this.known = known;
        this.groups = groups;
    }

    public static String getVersion() {
        return version;
    }

    public static void setVersion(String version) {
        CoordinationState.version = version;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, List<String>> getKnown() {
        return known;
    }

    public void setKnown(Map<String, List<String>> known) {
        this.known = known;
    }

    public List<String> getGroups() {
        return groups;
    }

    public void setGroups(List<String> groups) {
        this.groups = groups;
    }

    @Override
    public String toString() {
        return "CoordinationState [name=" + name + ", known=" + known + ", groups=" + groups + "]";
    }
}
