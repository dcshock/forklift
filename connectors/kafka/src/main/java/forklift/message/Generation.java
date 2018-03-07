package forklift.message;

/** Represents the abstract time during which a record belonging to a dataset was acquired. */
public class Generation {
    /** Number of the currently assigned generation, or -1 if there is no currently assigned generation */
    private volatile long generationNumber = -1;

    /** Generation number stored while unassigned, removes the need for read locking */
    private volatile long storedGenerationNumber  = -1;

    public boolean assigned() { return generationNumber != -1; }
    public long generationNumber() { return generationNumber; }

    public synchronized void assignNextGeneration() {
        this.storedGenerationNumber += 1;
        this.generationNumber = storedGenerationNumber;
    }

    public synchronized void unassign() {
        this.generationNumber = -1;
    }

    public boolean acceptsGeneration(final long otherGenerationNumber) {
        return otherGenerationNumber != -1 && otherGenerationNumber == generationNumber;
    }
}
