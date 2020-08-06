package forklift.source.sources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import forklift.source.LogicalSourceContext;
import forklift.source.SourceUtil;
import forklift.source.decorators.RoleInput;

public class RoleInputSourceTest {
    @Test
    public void testUndefinedRoles() {
        assertFalse(new RoleInputSource((String) null).isRoleDefined());
        assertFalse(new RoleInputSource("").isRoleDefined());
    }

    @Test
    public void testUndefinedRolesAreEqual() {
        assertEquals(new RoleInputSource((String) null), new RoleInputSource(""));
    }

    @Test
    public void testIsLogicalSource() {
        assertTrue(new RoleInputSource("TestRole").isLogicalSource());
    }

    @Test
    public void testActionSourceIsTakenFromContext() {
        final LogicalSourceContext testContext = source -> {
            return source
                .apply(RoleInputSource.class, roleSource -> new QueueSource("action-role-" + roleSource.getRole()))
                .get();
        };

        final RoleInputSource roleInput = new RoleInputSource("TestRole");

        assertEquals(new QueueSource("action-role-TestRole"), roleInput.getActionSource(testContext));
    }

    @Test
    public void testEqualityCorrespondsToRoleEquality() {
        final String testRole = "test-role";
        final String otherRole = "other-role";

        assertEquals(new RoleInputSource(testRole),
                            new RoleInputSource(testRole));
        assertNotEquals(new RoleInputSource(testRole),
                               new RoleInputSource(otherRole));
    }

    @Test
    public void testRoleInputHasDefaultRoleWithNoGivenRole() {
        final RoleInputSource source = (RoleInputSource) SourceUtil.getSources(DefaultRoleConsumer.class, RoleInputSource.class).findFirst().get();
        assertEquals("DefaultRoleConsumer", source.getRole());
    }

    @Test
    public void testRoleInputHasGivenRole() {
        final RoleInputSource source = (RoleInputSource) SourceUtil.getSources(TestRoleConsumer.class, RoleInputSource.class).findFirst().get();
        assertEquals("test-role", source.getRole());
    }

    @RoleInput
    public class DefaultRoleConsumer {}

    @RoleInput(role = "test-role")
    public class TestRoleConsumer {}
}
