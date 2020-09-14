package forklift.controller;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class GenerationTests {
    @Test
    public void acceptsCurrentGeneration() {
        final Generation generation = new Generation();
        generation.assignNextGeneration();

        final long currentGeneration = generation.generationNumber();
        assertTrue(generation.acceptsGeneration(currentGeneration));
    }

    @Test
    public void rejectsUnassignedGeneration() {
        final Generation generation = new Generation();
        generation.assignNextGeneration();
        generation.unassign();

        final long currentGeneration = generation.generationNumber();
        assertFalse(generation.acceptsGeneration(currentGeneration));

        // check that the number taken from the unassigned generation is still rejected later
        generation.assignNextGeneration();
        generation.unassign();

        assertFalse(generation.acceptsGeneration(currentGeneration));
    }

    @Test
    public void rejectsOldGeneration() {
        final Generation generation = new Generation();
        generation.assignNextGeneration();

        final long currentGeneration = generation.generationNumber();
        generation.unassign();
        generation.assignNextGeneration();

        assertFalse(generation.acceptsGeneration(currentGeneration));
    }
}
