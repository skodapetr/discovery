package com.linkedpipes.discovery.sample.store;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.DiscoveryListener;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.util.List;

/**
 * As data samples can be big we store them in this storage.
 * It it then responsibility of the storage to optimize stored
 * statements and make them available when needed.
 */
public interface SampleStore
        extends DiscoveryListener, Iterable<SampleStore.Entry> {

    class Entry {

        public final SampleRef ref;

        public final List<Statement> statements;

        public Entry(SampleRef ref, List<Statement> statements) {
            this.ref = ref;
            this.statements = statements;
        }

    }

    default SampleRef storeRoot(List<Statement> statements)
            throws DiscoveryException {
        return store(statements, SampleGroup.ROOT);
    }

    /**
     * Name can be used as an optional type identification.
     */
    SampleRef store(List<Statement> statements, SampleGroup group)
            throws DiscoveryException;

    /**
     * Store given data sample under given ref, used for transfers and
     * higher order stores.
     */
    void store(List<Statement> statements, SampleRef ref)
            throws DiscoveryException;

    List<Statement> load(SampleRef ref) throws DiscoveryException;

    void remove(SampleRef ref);

    void removeAll();

    static MemoryStore memoryStore() {
        return new MemoryStore();
    }

    static FileStorage fileSystemStore(File directory, MeterRegistry registry) {
        return new FileStorage(directory, registry);
    }

    static BreakupStore breakupStore(File directory, MeterRegistry registry) {
        return new BreakupStore(directory, registry);
    }

    static HierarchicalStore withCache(
            SampleStore cacheStore, SampleStore secondaryStore) {
        return new HierarchicalStore(cacheStore, secondaryStore);
    }

}
