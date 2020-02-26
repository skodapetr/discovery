package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.node.Node;

/**
 * Apply no filter.
 */
public class NoFilter implements NodeFilter {

    @Override
    public boolean isNewNode(Node node) {
        return true;
    }

}
