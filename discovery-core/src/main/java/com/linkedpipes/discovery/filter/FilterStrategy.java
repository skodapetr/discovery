package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.node.Node;

public interface FilterStrategy {

    default void init(Node root) {
        // No action.
    }

    default void addNode(Node node) {
        // No action.
    }

    boolean isNewNode(Node node);

}
