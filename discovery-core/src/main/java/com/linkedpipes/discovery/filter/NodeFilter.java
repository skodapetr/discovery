package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.node.Node;

public interface NodeFilter {

    default void init(Node root) throws DiscoveryException {
        // No action.
    }

    default void addNode(Node node) throws DiscoveryException {
        // No action.
    }

    boolean isNewNode(Node node) throws DiscoveryException;

    default void logAfterLevelFinished() {
        // No operation.
    }

}
