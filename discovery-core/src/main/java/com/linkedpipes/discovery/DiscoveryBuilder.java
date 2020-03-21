package com.linkedpipes.discovery;

import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.io.DiscoveryAdapter;
import com.linkedpipes.discovery.listeners.LimitByLevel;
import com.linkedpipes.discovery.listeners.LimitByNodeExpansionTime;
import com.linkedpipes.discovery.listeners.LimitByTime;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.AskNode;
import com.linkedpipes.discovery.node.ExpandNode;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.store.SampleRef;
import com.linkedpipes.discovery.sample.store.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.util.List;

/**
 * Create {@link Discovery}.
 */
public class DiscoveryBuilder {

    private String iri;

    private final List<Application> applications;

    private final List<Transformer> transformers;

    private Dataset dataset;

    private SampleStore store;

    private NodeFilter filter;

    private DataSampleTransformer dataSampleTransformer;

    private Integer levelLimit = null;

    private Integer timeLimitInMinutes = null;

    private Integer nodeLimitInMs = null;

    private MeterRegistry registry;

    public DiscoveryBuilder(
            String iri,
            List<Application> applications, List<Transformer> transformers) {
        this.iri = iri;
        this.applications = applications;
        this.transformers = transformers;
    }

    public void setLevelLimit(int levelLimit) {
        this.levelLimit = levelLimit;
    }

    public void setTimeLimitInMinutes(int timeInMinutes) {
        this.timeLimitInMinutes = timeInMinutes;
    }

    public void setMaxNodeExpansionTimeMs(int timeInMs) {
        this.nodeLimitInMs = timeInMs;
    }

    public void setStore(SampleStore store) {
        this.store = store;
    }

    public void setFilter(NodeFilter filter) {
        this.filter = filter;
    }

    public void setDataset(Dataset dataset) {
        this.dataset = dataset;
    }

    public void setDataSampleTransformer(
            DataSampleTransformer dataSampleTransformer) {
        this.dataSampleTransformer = dataSampleTransformer;
    }

    public void setRegistry(MeterRegistry registry) {
        this.registry = registry;
    }

    public Discovery createNew() throws DiscoveryException {
        validate();
        Discovery result = new Discovery(
                iri, applications, transformers,
                store, filter, dataSampleTransformer,
                registry);
        addListeners(result);
        initializeFromDataset(result);
        return result;
    }

    private void validate() throws DiscoveryException {
        if (store == null) {
            throw new DiscoveryException("Store is null.");
        }
        if (filter == null) {
            throw new DiscoveryException("Filter is null.");
        }
        if (dataSampleTransformer == null) {
            throw new DiscoveryException("Data sample transformer is null.");
        }
        if (registry == null) {
            throw new DiscoveryException("Registry is null.");
        }
    }

    private void initializeFromDataset(Discovery context)
            throws DiscoveryException {
        List<Statement> dataSample =
                dataSampleTransformer.transform(dataset.sample);
        createRoot(context, dataSample);
        expandRoot(context, dataSample);
    }

    private void createRoot(
            Discovery context, List<Statement> dataSample)
            throws DiscoveryException {
        SampleRef ref = store.storeRoot(dataSample);
        Node root = new Node();
        root.setDataSampleRef(ref);
        context.setRoot(root);
    }

    private void expandRoot(
            Discovery context, List<Statement> dataSample) {
        Node root = context.getRoot();
        createExpandNode().expandRoot(root, dataSample);
        context.getQueue().addAll(root.getNext());
    }

    private ExpandNode createExpandNode() {
        AskNode askNode = new AskNode(
                applications, transformers, registry);
        return new ExpandNode(
                store, filter, askNode, dataSampleTransformer, registry);
    }

    private void addListeners(Discovery context) {
        context.addListener(store);
        context.addListener(filter);
        context.addListener(dataSampleTransformer);
        if (timeLimitInMinutes != null) {
            context.addListener(new LimitByTime(timeLimitInMinutes));
        }
        if (nodeLimitInMs != null) {
            context.addListener(new LimitByNodeExpansionTime(nodeLimitInMs));
        }
        if (levelLimit != null) {
            context.addListener(new LimitByLevel(levelLimit));
        }
    }

    public Discovery resume(File directory) throws DiscoveryException {
        validate();
        Discovery result = new Discovery(
                iri, applications, transformers,
                store, filter, dataSampleTransformer,
                registry);
        addListeners(result);
        DiscoveryAdapter adapter = new DiscoveryAdapter();
        adapter.loadFromResume(directory, result);
        return result;
    }

}
