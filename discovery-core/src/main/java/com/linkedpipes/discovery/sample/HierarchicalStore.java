package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.SuppressFBWarnings;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.function.Function;

/**
 * Use two stores one as a cache (primary) and one as a secondary storage.
 * Whenever a data sample is a filter function is use to decide whether to
 * put the data into primary or secondary store.
 *
 * <p>If primary store is bigger then given level or memory utilization
 * all the data are moved to secondary store and primary store is no longer
 * used.
 */
class HierarchicalStore implements SampleStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(HierarchicalStore.class);

    private final float memoryUseLimit;

    private final SampleStore primaryStore;

    private final SampleStore secondaryStore;

    /**
     * When true primary store can be used.
     */
    private boolean usePrimaryStore = true;

    /**
     * When return true the primary store can be used to store this value.
     */
    private final Function<String, Boolean> primaryFilter;

    public HierarchicalStore(
            SampleStore cacheStore,
            SampleStore secondaryStore,
            float memoryUseLimit,
            Function<String, Boolean> cacheFilter) {
        this.primaryStore = cacheStore;
        this.secondaryStore = secondaryStore;
        this.memoryUseLimit = memoryUseLimit;
        this.primaryFilter = cacheFilter;
    }

    @Override
    public SampleRef store(List<Statement> statements, String name)
            throws DiscoveryException {
        checkStorageStrategy();
        if (usePrimaryStore && primaryFilter.apply(name)) {
            return primaryStore.store(statements, name);
        } else {
            return secondaryStore.store(statements, name);
        }
    }

    @Override
    public void store(List<Statement> statements, SampleRef ref)
            throws DiscoveryException {
        checkStorageStrategy();
        if (usePrimaryStore && primaryFilter.apply(ref.name)) {
            primaryStore.store(statements, ref);
        } else {
            secondaryStore.store(statements, ref);
        }
    }

    @SuppressFBWarnings(value = {"DM_GC"})
    private void checkStorageStrategy() throws DiscoveryException {
        // Check for memory use need to load from secondary memory.
        if (usePrimaryStore && shouldMoveToSecondaryStore()) {
            // Run GC to make sure no more ram then necessary is used.
            Runtime.getRuntime().gc();
            if (shouldMoveToSecondaryStore()) {
                // Ok moving.
                moveAllToSecondaryStore();
            }
        }
    }

    @Override
    public List<Statement> load(SampleRef ref) throws DiscoveryException {
        List<Statement> result;
        if (usePrimaryStore) {
            result = primaryStore.load(ref);
            if (result != null) {
                return result;
            }
        }
        result = secondaryStore.load(ref);
        return result;
    }

    // TODO Check after time, i.e. after certain number of inserts.
    private boolean shouldMoveToSecondaryStore() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory());
        float utilisation = ((float) usedMemory / runtime.maxMemory());
        if (utilisation < memoryUseLimit) {
            return false;
        }
        LOG.info("Clear cache on memory utilisation {}% ({} MB)",
                (int) (utilisation * 100), usedMemory / (1024 * 1024));
        return true;
    }

    /**
     * Move all statements from cacheStore to secondaryStore abd clean
     * the cache store.
     */
    private void moveAllToSecondaryStore() throws DiscoveryException {
        LOG.info("Moving cache to secondary store ...");
        usePrimaryStore = false;
        var iterator = primaryStore.iterate();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();
            secondaryStore.store(entry.statements, entry.ref);
        }
        primaryStore.cleanUp();
        LOG.info("Moving cache to secondary store ... done");
    }

    @Override
    public void releaseFromMemory(SampleRef ref) {
        if (!usePrimaryStore) {
            secondaryStore.releaseFromMemory(ref);
            return;
        }
        List<Statement> statements;
        try {
            statements = primaryStore.load(ref);
        } catch (Exception ex) {
            // Error getting data from primary store, we assume they
            // are in secondary store then.
            secondaryStore.releaseFromMemory(ref);
            return;
        }
        if (statements == null) {
            // Data are in secondary store.
            secondaryStore.releaseFromMemory(ref);
            return;
        }
        // Save data to secondary store.
        primaryStore.releaseFromMemory(ref);
        try {
            secondaryStore.store(statements, ref);
        } catch (DiscoveryException ex) {
            throw new RuntimeException("Can't save data.", ex);
        }
        ref.memoryCount -= 1;
        secondaryStore.releaseFromMemory(ref);
    }

    @Override
    public Iterator<Entry> iterate() throws DiscoveryException {
        Stack<Iterator<Entry>> iterators = new Stack<>();
        iterators.push(primaryStore.iterate());
        iterators.push(secondaryStore.iterate());
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return iterators.peek().hasNext();
            }

            @Override
            public Entry next() {
                if (!iterators.peek().hasNext()) {
                    iterators.pop();
                }
                if (iterators.empty()) {
                    throw new NoSuchElementException();
                }
                return iterators.peek().next();
            }

        };
    }

    @Override
    public void remove(SampleRef ref) {
        primaryStore.remove(ref);
        secondaryStore.remove(ref);
    }

    @Override
    public void cleanUp() {
        primaryStore.cleanUp();
        secondaryStore.cleanUp();
    }

    @Override
    public void logAfterLevelFinished() {
        if (usePrimaryStore) {
            primaryStore.logAfterLevelFinished();
        }
        secondaryStore.logAfterLevelFinished();
    }

}
