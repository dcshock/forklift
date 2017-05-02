package forklift.source.sources;

import forklift.source.SourceI;
import forklift.source.decorators.Topic;

import java.util.Objects;

/**
 * A more easily usable form of the annotation {@code @Topic}.
 */
public class TopicSource extends SourceI {
    private final String name;
    public TopicSource(String name) {
        this.name = name;
    }

    public TopicSource(Topic topic) {
        this.name = topic.value();
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof TopicSource))
            return false;
        TopicSource that = (TopicSource) o;

        return Objects.equals(this.name, that.name);
    }

    @Override
    public String toString() {
        return "TopicSource(" + name + ")";
    }
}
