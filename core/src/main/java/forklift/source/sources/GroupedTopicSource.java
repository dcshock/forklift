package forklift.source.sources;

import forklift.source.ActionSource;
import forklift.source.SourceI;
import forklift.source.decorators.GroupedTopic;

import java.util.Objects;

/**
 * A more easily usable form of the annotation {@code @GroupTopic}.
 */
public class GroupedTopicSource extends ActionSource {
    private final String name;
    private String group;

    public GroupedTopicSource(String name, String group) {
        this.name = name;
        this.group = group;
    }

    public GroupedTopicSource(GroupedTopic topic) {
        this.name = topic.name();
        this.group = topic.group();
    }

    public String getName() {
        return name;
    }

    public String getGroup() {
        return group;
    }

    /**
     * Determines whether or not the current group name is a null-like value, and should be changed.
     *
     * @return whether a group name is specified
     */
    public boolean groupSpecified() {
        return group != null && !group.isEmpty();
    }

    /**
     * Allow changing the group name here, so the group name can be ultimiately
     * determined by the connector.
     *
     * @param group the new group to use
     */
    public void overrideGroup(String group) {
        this.group = group;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, group);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof GroupedTopicSource))
            return false;
        GroupedTopicSource that = (GroupedTopicSource) o;

        return Objects.equals(this.name, that.name);
    }

    @Override
    public String toString() {
        return "GroupedTopicSource(name=" + name + ", group=" + group + ")";
    }
}
