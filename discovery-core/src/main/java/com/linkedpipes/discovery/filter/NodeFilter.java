package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.node.Node;
import org.eclipse.rdf4j.model.Statement;

import java.util.List;

public interface NodeFilter {

    default void init(Node root) throws DiscoveryException {
        // No action.
    }

    default void addNode(Node node) throws DiscoveryException {
        // No action.
    }

    boolean isNewNode(Node node, List<Statement> dataSample)
            throws DiscoveryException;

    default void logAfterLevelFinished() {
        // No operation.
    }

}
