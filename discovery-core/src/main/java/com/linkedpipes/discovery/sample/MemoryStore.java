package com.linkedpipes.discovery.sample;

import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

class MemoryStore implements SampleStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(MemoryStore.class);

    private Map<SampleRef, List<Statement>> store = new HashMap<>();

    private final boolean keepInMemory;

    private long size = 0;

    public MemoryStore(boolean keepInMemory) {
        this.keepInMemory = keepInMemory;
    }

    @Override
    public SampleRef store(List<Statement> statements, String name) {
        SampleRef ref = new SampleRef(name);
        store(statements, ref);
        return ref;
    }

    @Override
    public void store(List<Statement> statements, SampleRef ref) {
        size += statements.size();
        store.put(ref, statements);
    }

    @Override
    public List<Statement> load(SampleRef ref) {
        return store.get(ref);
    }

    @Override
    public void releaseFromMemory(SampleRef ref) {
        ref.memoryCount -= 1;
        if (keepInMemory) {
            return;
        }
        if (ref.memoryCount < 0) {
            size -= store.getOrDefault(ref, Collections.emptyList()).size();
            store.remove(ref);
        }
    }

    @Override
    public Iterator<Entry> iterate() {
        var iterator = store.entrySet().iterator();
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry next() {
                var entry = iterator.next();
                return new Entry(entry.getKey(), entry.getValue());
            }

        };
    }

    @Override
    public void remove(SampleRef ref) {
        store.remove(ref);
    }

    @Override
    public void cleanUp() {
        LOG.info("Clean up from {} to 0.", size);
        store.clear();
        size = 0;
    }

    @Override
    public void logAfterLevelFinished() {
        LOG.info("Stored statements: {}", size);
    }

}
