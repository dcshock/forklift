package forklift.controller;

import org.junit.Assert;
import org.junit.Test;

public class GenerationTests {
    @Test
    public void acceptsCurrentGeneration() {
        final Generation generation = new Generation();
        generation.assignNextGeneration();

        final long currentGeneration = generation.generationNumber();
        Assert.assertTrue(generation.acceptsGeneration(currentGeneration));
    }

    @Test
    public void rejectsUnassignedGeneration() {
        final Generation generation = new Generation();
        generation.assignNextGeneration();
        generation.unassign();

        final long currentGeneration = generation.generationNumber();
        Assert.assertFalse(generation.acceptsGeneration(currentGeneration));

        // check that the number taken from the unassigned generation is still rejected later
        generation.assignNextGeneration();
        generation.unassign();

        Assert.assertFalse(generation.acceptsGeneration(currentGeneration));
    }

    @Test
    public void rejectsOldGeneration() {
        final Generation generation = new Generation();
        generation.assignNextGeneration();

        final long currentGeneration = generation.generationNumber();
        generation.unassign();
        generation.assignNextGeneration();

        Assert.assertFalse(generation.acceptsGeneration(currentGeneration));
    }
}
