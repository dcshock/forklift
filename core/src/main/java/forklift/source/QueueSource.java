package forklift.source;

import forklift.source.decorators.Queue;

import java.util.Objects;

/**
 * A more easily usable form of the annotation {@code @Queue}.
 */
public class QueueSource implements SourceI {
    private final String name;
    public QueueSource(String name) {
        this.name = name;
    }

    public QueueSource(Queue queue) {
        this.name = queue.value();
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof QueueSource))
            return false;
        QueueSource that = (QueueSource) o;

        return Objects.equals(this.name, that.name);
    }

    @Override
    public String toString() {
        return "QueueSource(" + name + ")";
    }
}
