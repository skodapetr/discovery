package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.DiscoveryException;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.util.List;

/**
 * As data samples can be big we store them in this storage.
 * It it then responsibility of the storage to optimize stored
 * statements and make them available when needed.
 */
public interface SampleStore {

    /**
     * Name can be used as an optional type identification.
     */
    SampleRef store(List<Statement> statements, String name)
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
        ref.memoryCount -= 1;
        // Each interface implementation should provide custom method.
    }

    void cleanUp();

    default void logAfterLevelFinished() {
        // No operation.
    }

    static SampleStore memoryStore() {
        return new MemoryStore();
    }

    static SampleStore fileSystemStore(
            File directory, MeterRegistry registry) {
        return new FileStorage(directory, registry);
    }

}
