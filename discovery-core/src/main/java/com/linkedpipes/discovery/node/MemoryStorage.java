package com.linkedpipes.discovery.node;

import org.eclipse.rdf4j.model.Statement;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MemoryStorage implements DataSampleStorage {

    private final Map<Node, List<Statement>> storage = new HashMap<>();

    @Override
    public void setDataSample(Node node, List<Statement> dataSample) {
        storage.put(node, dataSample);
    }

    @Override
    public List<Statement> getDataSample(Node node) {
        return Collections.unmodifiableList(storage.get(node));
    }

    @Override
    public void deleteDataSample(Node node) {
        storage.remove(node);
    }

    @Override
    public void cleanUp() {
        storage.clear();
    }

}
