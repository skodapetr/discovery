package com.linkedpipes.discovery;

import com.linkedpipes.discovery.node.Node;

public interface DiscoveryListener {

    default boolean discoveryWillRun(Discovery context) {
        return true;
    }

    default void discoveryDidRun() {
        // No operation by default.
    }

    /**
     * Return false to stop discovery.
     */
    default boolean levelDidEnd(int level) {
        return true;
    }

    /**
     * Return false to stop discovery.
     */
    default boolean nodeWillExpand(Node node) {
        return true;
    }

    /**
     * Return false to stop discovery.
     */
    default boolean nodeDidExpand(Node node) {
        return true;
    }

    /**
     * Called when all resources connected to the discovery should be
     * released.
     */
    default void cleanUp() {
        // No operation by default.
    }

}
