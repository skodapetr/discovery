package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.sample.store.SampleRef;

import java.util.Collections;
import java.util.List;

/**
 * Represent a node in the exploration tree.
 */
public class Node {

    /**
     * Transformer applied to get to this node from {@link #previous}.
     */
    private final Transformer transformer;

    /**
     * Application that can be used in this node.
     */
    private List<Application> applications;

    private final Node previous;

    private List<Node> next = Collections.emptyList();

    private final int level;

    private SampleRef dataSampleRef = null;

    /**
     * True if the state of this node was explored before by other node.
     */
    private boolean redundant = false;

    /**
     * True if the node was visited and expanded.
     */
    private boolean expanded = false;

    public Node() {
        this.transformer = null;
        this.applications = Collections.emptyList();
        this.previous = null;
        this.level = 0;
    }

    public Node(Node previous, Transformer transformer) {
        this.transformer = transformer;
        this.applications = Collections.emptyList();
        this.previous = previous;
        this.level = previous.level + 1;
    }

    public Transformer getTransformer() {
        return transformer;
    }

    public List<Application> getApplications() {
        return Collections.unmodifiableList(applications);
    }

    public void setApplications(List<Application> applications) {
        this.applications = applications;
    }

    public Node getPrevious() {
        return previous;
    }

    public void setNext(List<Node> next) {
        this.next = next;
    }

    public List<Node> getNext() {
        return Collections.unmodifiableList(next);
    }

    public void addNext(Node node) {
        next.add(node);
    }

    public int getLevel() {
        return level;
    }

    public SampleRef getDataSampleRef() {
        return dataSampleRef;
    }

    public void setDataSampleRef(SampleRef dataSampleRef) {
        this.dataSampleRef = dataSampleRef;
    }

    public boolean isRedundant() {
        return redundant;
    }

    public void setRedundant(boolean redundant) {
        this.redundant = redundant;
    }

    public boolean isExpanded() {
        return expanded;
    }

    public void setExpanded(boolean expanded) {
        this.expanded = expanded;
    }

    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
        for (Node node : next) {
            node.accept(visitor);
        }
    }

}
