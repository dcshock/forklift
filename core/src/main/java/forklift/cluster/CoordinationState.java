package forklift.cluster;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CoordinationState {
    private static String version = "1";
    private String name;

    private Set<KnownInstance> known;

    private List<String> groups;

    public CoordinationState () {}

    public CoordinationState(String name) {
        this.name = name;
        this.known = new HashSet<>();
        this.groups = new ArrayList<>();
    }

    public CoordinationState(String name, Set<KnownInstance> known, List<String> groups) {
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

    public Set<KnownInstance> getKnown() {
        return known;
    }

    public void setKnown(Set<KnownInstance> known) {
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
