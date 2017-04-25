package forklift.replay;

import forklift.decorators.ConsumerRole;
import forklift.source.SourceI;

import java.util.Objects;

public class ReplaySource extends SourceI {
    private String role;
    public ReplaySource(String role) {
        this.role = role;
    }

    public ReplaySource(Replay replay) {
        this.role = replay.role();
    }

    @Override
    protected void onContextSet() {
        // if the role wasn't set by the source annotation, look for a ConsumerRole
        if (!isRoleDefined()) {
            ConsumerRole consumerRole = getContextClass().getAnnotation(ConsumerRole.class);
            if (consumerRole != null) {
                this.role = consumerRole.name();
            }
        }

        // if that didn't work either, fallback to the classname
        if (!isRoleDefined()) {
            this.role = getContextClass().getSimpleName();
        }
    }

    public boolean isRoleDefined() {
        return role != null && !role.isEmpty();
    }

    public String getRole() {
        if (!isRoleDefined()) {
            return null;
        }
        return role;
    }

    @Override
    public String toString() {
        return "ReplaySource(role=" + role + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof ReplaySource))
            return false;
        ReplaySource that = (ReplaySource) o;

        return Objects.equals(this.getRole(), that.getRole());
    }
}
