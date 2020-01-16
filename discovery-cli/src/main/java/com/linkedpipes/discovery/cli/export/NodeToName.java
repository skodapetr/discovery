package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.node.Node;

import java.util.HashMap;
import java.util.Map;

public class NodeToName {

    /**
     * Start on -1, as 0 is root and 1 is the first node.
     */
    private int counter = -1;

    private final Map<Node, String> names = new HashMap<>();

    public NodeToName(Node root) {
        root.accept((node) -> names.put(node, nextName()));
        // Force root to be root.
        names.put(root, "root_0000");
    }

    private String nextName() {
        return "node_" + String.format("%05d", ++counter);
    }

    public String name(Node node) {
        return names.get(node);
    }

}

