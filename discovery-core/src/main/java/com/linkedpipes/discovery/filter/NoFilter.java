package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.node.Node;
import org.eclipse.rdf4j.model.Statement;

import java.util.List;

/**
 * All is new, i.e. no filter.
 */
public class NoFilter implements NodeFilter {

    @Override
    public boolean isNewNode(Node node, List<Statement> dataSample) {
        return true;
    }
}
