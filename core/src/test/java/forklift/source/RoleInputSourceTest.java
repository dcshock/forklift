package forklift.source;

import forklift.source.decorators.RoleInput;

import org.junit.Assert;
import org.junit.Test;

public class RoleInputSourceTest {
    @Test
    public void testUndefinedRoles() {
        Assert.assertFalse(new RoleInputSource((String) null).isRoleDefined());
        Assert.assertFalse(new RoleInputSource("").isRoleDefined());
    }

    @Test
    public void testUndefinedRolesAreEqual() {
        Assert.assertEquals(new RoleInputSource((String) null),
                            new RoleInputSource(""));
    }

    @Test
    public void testEqualityCorrespondsRoleEquality() {
        final String testRole = "test-role";
        final String otherRole = "other-role";

        Assert.assertEquals(new RoleInputSource(testRole),
                            new RoleInputSource(testRole));
        Assert.assertNotEquals(new RoleInputSource(testRole),
                               new RoleInputSource(otherRole));
    }

    @Test
    public void testRoleInputHasDefaultRoleWithNoGivenRole() {
        final RoleInputSource source = (RoleInputSource) SourceUtil.getSources(DefaultRoleConsumer.class, RoleInputSource.class).findFirst().get();
        Assert.assertEquals("DefaultRoleConsumer", source.getRole());
    }

    @Test
    public void testRoleInputHasGivenRole() {
        final RoleInputSource source = (RoleInputSource) SourceUtil.getSources(TestRoleConsumer.class, RoleInputSource.class).findFirst().get();
        Assert.assertEquals("test-role", source.getRole());
    }

    @RoleInput
    public class DefaultRoleConsumer {}

    @RoleInput(role = "test-role")
    public class TestRoleConsumer {}
}
