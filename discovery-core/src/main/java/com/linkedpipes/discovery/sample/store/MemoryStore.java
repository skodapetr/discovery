package com.linkedpipes.discovery.sample.store;

import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

public class MemoryStore implements SampleStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(MemoryStore.class);

    private Map<SampleRef, List<Statement>> store = new HashMap<>();

    private long size = 0;

    @Override
    public SampleRef store(List<Statement> statements, SampleGroup group) {
        SampleRef ref = new SampleRef(group);
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
    public Iterator<Entry> iterator() {
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
        size -= store.getOrDefault(ref, Collections.emptyList()).size();
        store.remove(ref);
    }

    @Override
    public void removeAll() {
        LOG.info("Removing all {} statements.", size);
        store.clear();
        size = 0;
    }

    @Override
    public boolean levelDidEnd(int level) {
        LOG.info("Stored statements: {}", size);
        return true;
    }

}
