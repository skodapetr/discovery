package com.linkedpipes.discovery.sample;

/**
 * Reference used to identify a collection of statements.
 */
public class SampleRef {

    /**
     * Number of references of this reference (data sample) that need it
     * in main memory.
     */
    int memoryCount = 1;

    final String name;

    public SampleRef(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "SampleRef: " + name;
    }

}
