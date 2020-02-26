package com.linkedpipes.discovery.sample;

import org.eclipse.rdf4j.model.Statement;

import java.util.List;
import java.util.HashMap;
import java.util.Map;

class MemoryStore implements SampleStore {

    private Map<SampleRef, List<Statement>> store = new HashMap<>();

    @Override
    public SampleRef store(List<Statement> statements, String name) {
        SampleRef ref = new SampleRef();
        store.put(ref, statements);
        return ref;
    }

    @Override
    public List<Statement> load(SampleRef ref) {
        return store.get(ref);
    }

    @Override
    public void cleanUp() {
        store.clear();
    }

}
