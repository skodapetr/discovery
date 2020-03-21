package com.linkedpipes.discovery.sample.store;

public enum SampleGroup {
    /**
     * Represent root data sample.
     */
    ROOT,
    /**
     * Represent data sample used by filter, these should
     * remain in memory if possible.
     */
    FILTER,
    /**
     * Represent a node data sample, these have lowest priority to remain
     * in main memory.
     */
    NODE;

}
