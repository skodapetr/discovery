package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.sample.SampleRef;

import java.util.Collections;
import java.util.List;

/**
 * Represent a node in the exploration tree.
 */
public class Node {

    private final List<Dataset> sources;

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

    private final SampleRef dataSampleRef;

    public Node(List<Dataset> sources, SampleRef dataSampleRef) {
        this.sources = sources;
        this.transformer = null;
        this.applications = Collections.emptyList();
        this.previous = null;
        this.level = 0;
        this.dataSampleRef = dataSampleRef;
    }

    public Node(
            Node previous, Transformer transformer,
            SampleRef dataSampleRef) {
        this.sources = previous.sources;
        this.transformer = transformer;
        this.applications = Collections.emptyList();
        this.previous = previous;
        this.level = previous.level + 1;
        this.dataSampleRef = dataSampleRef;
    }

    public List<Dataset> getSources() {
        return Collections.unmodifiableList(sources);
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

    public boolean hasApplication() {
        return !applications.isEmpty();
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

    public int getLevel() {
        return level;
    }

    public SampleRef getDataSampleRef() {
        return dataSampleRef;
    }

    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
        for (Node node : next) {
            node.accept(visitor);
        }
    }

}
