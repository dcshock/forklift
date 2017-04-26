package forklift.source;

import forklift.source.decorators.RoleInput;

import java.util.Objects;

/**
 * Represents a source of messages for a consumer with the given named role.
 */
public class RoleInputSource extends SourceI {
    private String role;
    public RoleInputSource(String role) {
        this.role = role;
    }

    public RoleInputSource(RoleInput roleInput) {
        this.role = roleInput.role();
    }

    @Override
    protected void onContextSet() {
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
        return "RoleInputSource(role=" + getRole() + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RoleInputSource))
            return false;

        RoleInputSource that = (RoleInputSource) o;
        return Objects.equals(this.getRole(), that.getRole());
    }
}
