package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.DiscoveryException;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * As data samples can be big we store them in this storage.
 * It it then responsibility of the storage to optimize stored
 * statements and make them available when needed.
 */
public interface SampleStore {

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
        return store(statements, "root");
    }

    /**
     * Name can be used as an optional type identification.
     */
    SampleRef store(List<Statement> statements, String name)
            throws DiscoveryException;

    /**
     * Store given data sample under given ref, used for transfers and
     * higher order stores.
     */
    void store(List<Statement> statements, SampleRef ref)
            throws DiscoveryException;

    List<Statement> load(SampleRef ref) throws DiscoveryException;

    /**
     * Register another user of the reference. The number of deletes function
     * calls must be same as the number of reference users in order to
     * remove the data.
     */
    default void addReferenceUser(SampleRef ref) {
        ref.memoryCount += 1;
    }

    /**
     * Should be called, when the data sample is no longer needed by its user.
     * Similar to delete all user must agree in order to release the
     * data sample from memory.
     */
    default void releaseFromMemory(SampleRef ref) {
        // Each interface implementation should provide custom method.
        ref.memoryCount -= 1;
    }

    /**
     * Remove the referenced data sample from the store.
     */
    void remove(SampleRef ref);

    Iterator<Entry> iterate() throws DiscoveryException;

    void cleanUp();

    default void logAfterLevelFinished() {
        // No operation by default.
    }

    static SampleStore memoryStore(boolean keepInMemory) {
        return new MemoryStore(keepInMemory);
    }

    static SampleStore fileSystemStore(File directory, MeterRegistry registry) {
        return new FileStorage(directory, registry);
    }

    static SampleStore breakupStore(File directory, MeterRegistry registry) {
        return new BreakupStore(directory, registry);
    }

    static SampleStore withCache(
            float memoryUseLimit, Function<String, Boolean> cacheFilter,
            SampleStore cacheStore, SampleStore secondaryStore) {
        return new HierarchicalStore(
                cacheStore, secondaryStore, memoryUseLimit, cacheFilter);
    }

    static SampleStore diffStore(boolean keepInMemory, MeterRegistry registry) {
        return new DiffStore(keepInMemory, registry);
    }

}
