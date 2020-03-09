package com.linkedpipes.discovery.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Remove all nodes without data sample or redundant nodes.
 */
public class ShakeNonExpandedNodes implements NodeVisitor {

    private static final Logger LOG =
            LoggerFactory.getLogger(ShakeNonExpandedNodes.class);

    private int before = 0;

    private int after = 0;

    public void shake(Node root) {
        root.accept(this);
        LOG.info("Shaking done {} -> {}", before, after);
    }

    @Override
    public void visit(Node node) {
        before += node.getNext().size();
        var next = node.getNext().stream()
                .filter(Node::isExpanded)
                .collect(Collectors.toList());
        after += next.size();
        node.setNext(next);
    }

}
