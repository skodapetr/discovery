package com.linkedpipes.discovery.sample.store;

/**
 * Reference used to identify a collection of statements.
 */
public class SampleRef {

    final SampleGroup group;

    public SampleRef(SampleGroup group) {
        this.group = group;
    }

    public SampleGroup getGroup() {
        return group;
    }

    @Override
    public String toString() {
        return "SampleRef: " + group.name();
    }

}
