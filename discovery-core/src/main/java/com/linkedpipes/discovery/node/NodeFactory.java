package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import org.eclipse.rdf4j.model.Statement;

import java.util.ArrayList;
import java.util.List;

public class NodeFactory {

    private final DataSampleStorage sampleService;

    public NodeFactory(DataSampleStorage sampleService) {
        this.sampleService = sampleService;
    }

    public Node createNode(List<Dataset> sources) throws DiscoveryException {
        Node node = new Node(sources);
        List<Statement> dataSample = new ArrayList<>();
        sources.forEach((source) -> dataSample.addAll(source.sample));
        sampleService.setDataSample(node, dataSample);
        return node;
    }

    public Node createNode(
            Node previous,
            Transformer transformer,
            List<Statement> dataSample) throws DiscoveryException {
        Node node = new Node(previous, transformer);
        sampleService.setDataSample(node, dataSample);
        return node;
    }

}
