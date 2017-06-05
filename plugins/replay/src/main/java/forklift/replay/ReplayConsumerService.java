package forklift.replay;

import forklift.decorators.BeanResolver;

public class ReplayConsumerService {
    private final ReplayESWriter writer;

    public ReplayConsumerService(ReplayESWriter writer) {
        this.writer = writer;
    }

    @BeanResolver
    public Object resolve(Class<?> c, String name) {
        if (c == ReplayESWriter.class)
            return this.writer;

        return null;
    }
}
