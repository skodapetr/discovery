package com.linkedpipes.discovery;

import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.store.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * Holds all data related to the discovery.
 */
public class Discovery {

    private final String iri;

    private Node root;

    private final Deque<Node> queue = new ArrayDeque<>();

    private final List<DiscoveryListener> listeners = new ArrayList<>();

    private final List<Application> applications;

    private final List<Transformer> transformers;

    private final SampleStore store;

    private final NodeFilter filter;

    private final DataSampleTransformer dataSampleTransformer;

    private final MeterRegistry registry;

    public Discovery(
            String iri,
            List<Application> applications, List<Transformer> transformers,
            SampleStore store, NodeFilter filter,
            DataSampleTransformer dataSampleTransformer,
            MeterRegistry registry) {
        this.iri = iri;
        this.applications = applications;
        this.transformers = transformers;
        this.store = store;
        this.filter = filter;
        this.dataSampleTransformer = dataSampleTransformer;
        this.registry = registry;
    }

    public String getIri() {
        return iri;
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public Deque<Node> getQueue() {
        return queue;
    }

    public List<DiscoveryListener> getListeners() {
        return Collections.unmodifiableList(listeners);
    }

    public void addListener(DiscoveryListener listener) {
        listeners.add(listener);
    }

    public List<Application> getApplications() {
        return applications;
    }

    public List<Transformer> getTransformers() {
        return transformers;
    }

    public SampleStore getStore() {
        return store;
    }

    public NodeFilter getFilter() {
        return filter;
    }

    public DataSampleTransformer getDataSampleTransformer() {
        return dataSampleTransformer;
    }

    public MeterRegistry getRegistry() {
        return registry;
    }

    public void cleanUp() {
        filter.cleanUp();
        store.cleanUp();
        dataSampleTransformer.cleanUp();
    }

}
