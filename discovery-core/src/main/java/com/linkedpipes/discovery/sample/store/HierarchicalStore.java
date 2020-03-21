package com.linkedpipes.discovery.sample.store;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.node.Node;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Stack;

/**
 * Use two stores one as a cache (primary) and one as a secondary storage.
 * Whenever a data sample is a filter function is use to decide whether to
 * put the data into primary or secondary store.
 *
 * <p>If primary store is bigger then given level or memory utilization
 * all the data are moved to secondary store and primary store is no longer
 * used.
 */
public class HierarchicalStore implements SampleStore {

    @FunctionalInterface
    public interface GroupFilter {

        boolean apply(SampleGroup group, List<Statement> dataSample);

    }

    private static final Logger LOG =
            LoggerFactory.getLogger(HierarchicalStore.class);

    private final SampleStore primaryStore;

    private final SampleStore secondaryStore;

    /**
     * When true primary store can be used.
     */
    private boolean usePrimaryStore = true;

    /**
     * Return sample importance, lower values have higher chance
     * to remain in primary store.
     */
    private GroupFilter groupFilter = (group, dataSample) -> true;

    public HierarchicalStore(
            SampleStore cacheStore,
            SampleStore secondaryStore) {
        this.primaryStore = cacheStore;
        this.secondaryStore = secondaryStore;
    }

    @Override
    public SampleRef store(List<Statement> statements, SampleGroup group)
            throws DiscoveryException {
        if (usePrimaryStore) {
            return primaryStore.store(statements, group);
        } else {
            return secondaryStore.store(statements, group);
        }
    }

    @Override
    public void store(List<Statement> statements, SampleRef ref)
            throws DiscoveryException {
        if (usePrimaryStore) {
            primaryStore.store(statements, ref);
        } else {
            secondaryStore.store(statements, ref);
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

    @Override
    public Iterator<Entry> iterator() {
        Stack<Iterator<Entry>> iterators = new Stack<>();
        iterators.push(primaryStore.iterator());
        iterators.push(secondaryStore.iterator());
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
    public void removeAll() {
        primaryStore.cleanUp();
        secondaryStore.cleanUp();
    }

    @Override
    public boolean discoveryWillRun(Discovery context) {
        boolean result = true;
        result &= primaryStore.discoveryWillRun(context);
        result &= secondaryStore.discoveryWillRun(context);
        return result;
    }

    @Override
    public void discoveryDidRun() {
        primaryStore.discoveryDidRun();
        secondaryStore.discoveryDidRun();
    }

    @Override
    public boolean levelDidEnd(int level) {
        boolean result = true;
        result &= primaryStore.levelDidEnd(level);
        result &= secondaryStore.levelDidEnd(level);
        return result;
    }

    @Override
    public boolean nodeWillExpand(Node node) {
        boolean result = true;
        result &= primaryStore.nodeWillExpand(node);
        result &= secondaryStore.nodeWillExpand(node);
        return result;
    }

    /**
     * With new function move data from primary store.
     */
    public void optimizeStore(GroupFilter groupFilter)
            throws DiscoveryException {
        LOG.info("Optimizing store ...");
        List<SampleRef> toBeMoved = new ArrayList<>();
        for (Entry entry : primaryStore) {
            if (groupFilter.apply(entry.ref.group, entry.statements)) {
                continue;
            }
            toBeMoved.add(entry.ref);
        }
        for (SampleRef sampleRef : toBeMoved) {
            secondaryStore.store(primaryStore.load(sampleRef), sampleRef);
            primaryStore.remove(sampleRef);
        }
        LOG.info("Optimizing store ... done (moved: {})", toBeMoved.size());
    }

    @Override
    public boolean nodeDidExpand(Node node) {
        boolean result = true;
        result &= primaryStore.nodeDidExpand(node);
        result &= secondaryStore.nodeDidExpand(node);
        // We can not save node data example, as the data sample
        // is used by all it's children.
        return result;
    }

}
