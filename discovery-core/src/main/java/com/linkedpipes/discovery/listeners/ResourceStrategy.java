package com.linkedpipes.discovery.listeners;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.DiscoveryListener;
import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.store.HierarchicalStore;
import com.linkedpipes.discovery.sample.store.SampleGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ResourceStrategy implements DiscoveryListener {

    public static class HierarchicalStoreOptimization {

        private final Supplier<Boolean> condition;

        private final HierarchicalStore.GroupFilter filter;

        public HierarchicalStoreOptimization(
                Supplier<Boolean> condition,
                HierarchicalStore.GroupFilter filter) {
            this.condition = condition;
            this.filter = filter;
        }

    }

    private static final Logger LOG =
            LoggerFactory.getLogger(ResourceStrategy.class);

    private static final long MB = 1024 * 1024;

    private final List<HierarchicalStoreOptimization>
            storeOptimizations = new ArrayList<>();

    private int nextStoreOptimizations = 0;

    private HierarchicalStore store;

    public void setStore(HierarchicalStore store) {
        this.store = store;
    }

    /**
     * Optimizations need to be add from the least restrictive.
     */
    public void addHierarchicalStoreOptimization(
            HierarchicalStoreOptimization store) {
        storeOptimizations.add(store);
    }

    @Override
    public boolean nodeWillExpand(Node node) {
        try {
            checkStorageStrategy(0);
        } catch (DiscoveryException ex) {
            LOG.error("Storage optimization failed.", ex);
        }
        return true;
    }

    @SuppressFBWarnings(value = {"DM_GC"})
    private void checkStorageStrategy(int checkCounter)
            throws DiscoveryException {
        for (int index = nextStoreOptimizations;
                index < storeOptimizations.size(); ++index) {
            var optimization = storeOptimizations.get(index);
            if (!optimization.condition.get()) {
                break;
            }
            // Run GC and test again.
            Runtime.getRuntime().gc();
            if (!optimization.condition.get()) {
                break;
            }
            store.optimizeStore(optimization.filter);
            nextStoreOptimizations = index + 1;
            // We do not break here, instead we continue trying as
            // we may need to apply multiple optimizations.
        }
    }

    public static float memoryUtilizationInPercent() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory());
        return ((float) usedMemory / runtime.maxMemory());
    }

    public static long memoryFreeInMb() {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = (runtime.totalMemory() - runtime.freeMemory());
        long maxMemory = runtime.maxMemory();
        return (maxMemory - usedMemory) / MB;
    }

    /**
     * Move all node data samples into secondary memory.
     */
    public static HierarchicalStoreOptimization moveNode(
            Supplier<Boolean> condition) {
        return new HierarchicalStoreOptimization(
                condition, (group, dataSample) -> group != SampleGroup.NODE
        );
    }

    /**
     * Move all node and big filter data samples to secondary memory.
     */
    public static HierarchicalStoreOptimization moveNodeAndBigDiff(
            Supplier<Boolean> condition, int sizeThreshold) {
        return new HierarchicalStoreOptimization(
                condition,
                (group, dataSample) -> {
                    switch (group) {
                        case ROOT:
                            return true;
                        case FILTER:
                            return dataSample.size() > sizeThreshold;
                        case NODE:
                        default:
                            return false;

                    }
                });
    }

    /**
     * Move all to secondary memory.
     */
    public static HierarchicalStoreOptimization moveAll(
            Supplier<Boolean> condition) {
        return new HierarchicalStoreOptimization(
                condition, (group, dataSample) -> false
        );
    }

}
