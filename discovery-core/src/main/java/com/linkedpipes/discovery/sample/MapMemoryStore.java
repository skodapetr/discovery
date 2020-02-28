package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.MeterNames;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * When executing SPARQL updates new statements are always created
 * although they have the same content - they are equal.
 *
 * <p>As many transformers do modify only small part of the sample, there
 * is a huge duplicity. We try to mitigate this in this storage.
 */
class MapMemoryStore implements SampleStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(MapMemoryStore.class);

    /**
     * We utilize underlying memory store, we just replace tha statements
     * upon savings.
     */
    private MemoryStore store = new MemoryStore();

    private Map<Statement, Statement> known = new HashMap<>();

    private final Timer timer;

    public MapMemoryStore(MeterRegistry registry) {
        this.timer = registry.timer(MeterNames.STORE_MAP_MEMORY);
    }

    @Override
    public SampleRef store(List<Statement> statements, String name) {
        Instant start = Instant.now();
        addToKnown(statements);
        List<Statement> mappedStatements = mapStatements(statements);
        timer.record(Duration.between(start, Instant.now()));
        return store.store(mappedStatements, name);
    }

    private void addToKnown(List<Statement> statements) {
        for (Statement statement : statements) {
            known.putIfAbsent(statement, statement);
        }
    }

    private List<Statement> mapStatements(List<Statement> statements) {
        return statements.stream()
                .map(st -> known.get(st))
                .collect(Collectors.toList());
    }

    @Override
    public List<Statement> load(SampleRef ref) {
        return store.load(ref);
    }

    @Override
    public void addReferenceUser(SampleRef ref) {
        store.addReferenceUser(ref);
    }

    @Override
    public void releaseFromMemory(SampleRef ref) {
        store.releaseFromMemory(ref);
    }

    @Override
    public void cleanUp() {
        store.cleanUp();
        known.clear();
    }

    @Override
    public void logAfterLevelFinished() {
        LOG.info("Known statements size: {}", known.size());
        store.logAfterLevelFinished();
    }

}
