package com.linkedpipes.discovery.node;

@FunctionalInterface
public interface NodeVisitor {

    void visit(Node node);

}
