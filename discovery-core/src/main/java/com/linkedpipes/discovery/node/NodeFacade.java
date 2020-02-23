package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.util.List;

public class NodeFacade {

    private final NodeFactory nodeFactory;

    private final DataSampleStorage storage;

    protected NodeFacade(NodeFactory nodeFactory, DataSampleStorage storage) {
        this.nodeFactory = nodeFactory;
        this.storage = storage;
    }

    public Node createNode(List<Dataset> sources) throws DiscoveryException {
        return nodeFactory.createNode(sources);
    }

    public Node createNode(
            Node previous,
            Transformer transformer,
            List<Statement> dataSample) throws DiscoveryException {
        return nodeFactory.createNode(previous, transformer, dataSample);
    }

    public List<Statement> getDataSample(Node node)
            throws DiscoveryException {
        return storage.getDataSample(node);
    }

    public void deleteDataSample(Node node) {
        storage.deleteDataSample(node);
    }

    public void cleanUp() throws DiscoveryException {
        storage.cleanUp();
    }

    public static NodeFacade withMemoryStorage() {
        DataSampleStorage storage = new MemoryStorage();
        return new NodeFacade(new NodeFactory(storage), storage);
    }

    public static NodeFacade withFileSystemStorage(
            File directory, MeterRegistry registry) {
        directory.mkdirs();
        DataSampleStorage storage = new FileStorage(directory, registry);
        return new NodeFacade(new NodeFactory(storage), storage);
    }

}
