package forklift.replay;

import forklift.decorators.ConsumerRole;
import forklift.source.SourceI;

import org.junit.Assert;
import org.junit.Test;

public class ReplaySourceTest {
    /*
     * Equality tests (really obvious stuff)
     */
    @Test
    public void testEqualsWorksNormally() {
        final String testRole = "test-role";

        Assert.assertEquals(new ReplaySource(testRole),
                            new ReplaySource(testRole));
    }

    @Test
    public void testDifferentReplaySourcesAreNotEqual() {
        Assert.assertNotEquals(new ReplaySource("test-role"),
                               new ReplaySource("other-role"));
    }

    @Test
    public void testUndefinedReplaySourcesAreEqual() {
        Assert.assertEquals(new ReplaySource(""),
                            new ReplaySource((String) null));
    }

    @Test
    public void testSourceWithNoRoleHasUndefinedRole() {
        Assert.assertFalse(new ReplaySource("").isRoleDefined());
        Assert.assertFalse(new ReplaySource((String) null).isRoleDefined());
    }

    /*
     * Test inializing ReplaySource with context works as expected
     */
    @Test
    public void testClassWithNoRoleSetHasClassnameRole() {
        final ReplaySource source = (ReplaySource) SourceI.getSources(UnnamedConsumer.class).get(0);

        Assert.assertTrue(source.isRoleDefined());
        Assert.assertEquals("UnnamedConsumer", source.getRole());
    }

    @Test
    public void testClassWithReplayRoleHasGivenRole() {
        final ReplaySource source = (ReplaySource) SourceI.getSources(ReplayRoleConsumer.class).get(0);

        Assert.assertTrue(source.isRoleDefined());
        Assert.assertEquals("test-role", source.getRole());
    }

    @Test
    public void testClassWithConsumerRoleHasGivenRole() {
        final ReplaySource source = (ReplaySource) SourceI.getSources(ConsumerRoleConsumer.class).get(0);

        Assert.assertTrue(source.isRoleDefined());
        Assert.assertEquals("test-consumer-role", source.getRole());
    }

    @Test
    public void testClassWithMultipleRolesUsesReplayRole() {
        final ReplaySource source = (ReplaySource) SourceI.getSources(MultipleRoleConsumer.class).get(0);

        Assert.assertTrue(source.isRoleDefined());
        Assert.assertEquals("replay-role", source.getRole());
    }

    @Replay
    class UnnamedConsumer {}

    @Replay(role = "test-role")
    class ReplayRoleConsumer {}

    @ConsumerRole(name = "test-consumer-role")
    @Replay
    class ConsumerRoleConsumer {}

    @ConsumerRole(name = "consumer-role")
    @Replay(role = "replay-role")
    class MultipleRoleConsumer {}
}
