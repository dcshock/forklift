package forklift.source.sources;

import forklift.source.LogicalSourceContext;
import forklift.source.SourceUtil;
import forklift.source.decorators.RoleInput;

import org.junit.Assert;
import org.junit.Test;

public class RoleInputSourceTest {
    
    public void testUndefinedRoles() {
        Assert.assertFalse(new RoleInputSource((String) null).isRoleDefined());
        Assert.assertFalse(new RoleInputSource("").isRoleDefined());
    }

    
    public void testUndefinedRolesAreEqual() {
        Assert.assertEquals(new RoleInputSource((String) null),
                            new RoleInputSource(""));
    }

    
    public void testIsLogicalSource() {
        Assert.assertTrue(new RoleInputSource("TestRole").isLogicalSource());
    }

    
    public void testActionSourceIsTakenFromContext() {
        final LogicalSourceContext testContext = source -> {
            return source
                .apply(RoleInputSource.class, roleSource -> new QueueSource("action-role-" + roleSource.getRole()))
                .get();
        };

        final RoleInputSource roleInput = new RoleInputSource("TestRole");

        Assert.assertEquals(new QueueSource("action-role-TestRole"),
                            roleInput.getActionSource(testContext));
    }

    
    public void testEqualityCorrespondsToRoleEquality() {
        final String testRole = "test-role";
        final String otherRole = "other-role";

        Assert.assertEquals(new RoleInputSource(testRole),
                            new RoleInputSource(testRole));
        Assert.assertNotEquals(new RoleInputSource(testRole),
                               new RoleInputSource(otherRole));
    }

    
    public void testRoleInputHasDefaultRoleWithNoGivenRole() {
        final RoleInputSource source = (RoleInputSource) SourceUtil.getSources(DefaultRoleConsumer.class, RoleInputSource.class).findFirst().get();
        Assert.assertEquals("DefaultRoleConsumer", source.getRole());
    }

    
    public void testRoleInputHasGivenRole() {
        final RoleInputSource source = (RoleInputSource) SourceUtil.getSources(TestRoleConsumer.class, RoleInputSource.class).findFirst().get();
        Assert.assertEquals("test-role", source.getRole());
    }

    @RoleInput
    public class DefaultRoleConsumer {}

    @RoleInput(role = "test-role")
    public class TestRoleConsumer {}
}
