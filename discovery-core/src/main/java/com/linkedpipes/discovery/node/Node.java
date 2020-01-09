package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import org.eclipse.rdf4j.model.Statement;

import java.util.ArrayList;
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

    /**
     * Data sample in this node.
     */
    private final List<Statement> dataSample;

    private final Node previous;

    private List<Node> next = Collections.emptyList();

    private final int level;

    public Node(List<Dataset> sources) {
        this.sources = sources;
        this.transformer = null;
        this.applications = Collections.emptyList();
        this.dataSample = new ArrayList<>();
        sources.forEach((source) -> this.dataSample.addAll(source.sample));
        this.previous = null;
        this.level = 0;
    }

    public Node(
            Node previous,
            Transformer transformer,
            List<Statement> dataSample) {
        this.sources = previous.sources;
        this.transformer = transformer;
        this.applications = Collections.emptyList();
        this.dataSample = dataSample;
        this.previous = previous;
        this.level = previous.level + 1;
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

    public List<Statement> getDataSample() {
        return Collections.unmodifiableList(dataSample);
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

    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
        for (Node node : next) {
            node.accept(visitor);
        }
    }

}
