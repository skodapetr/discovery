package com.linkedpipes.discovery.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DataSampleDiff;
import com.linkedpipes.discovery.sample.store.SampleGroup;
import com.linkedpipes.discovery.sample.store.SampleRef;
import com.linkedpipes.discovery.sample.store.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DiffBasedFilter implements NodeFilter {

    public static class UsageReport {

        /**
         * Number of times filter on given level wa used.
         */
        int used = 0;

        /**
         * Number of times filter match given data sample.
         */
        int match = 0;

        /**
         * Duration in ms of filtering using given filter.
         */
        long duration = 0;

    }

    /**
     * We need to store information about added and removed
     * statements as a single diff would not capture that.
     */
    private static class DiffRef {

        public final SampleRef added;

        public final SampleRef removed;

        public final Integer size;

        public DiffRef(SampleRef added, SampleRef removed, Integer size) {
            this.added = added;
            this.removed = removed;
            this.size = size;
        }

    }

    private static class IoContainer {

        public List<String> added = new ArrayList<>();

        public List<String> removed = new ArrayList<>();

        public List<Integer> sizes = new ArrayList<>();

    }

    public static final String REF_NAME = "diff";

    private static final Logger LOG =
            LoggerFactory.getLogger(DiffBasedFilter.class);

    /**
     * We keep the root sample in main memory as we use it very often.
     */
    private Set<Statement> root;

    /**
     * Store NodeDiffs in lists by size.
     */
    private final Map<Integer, List<DiffRef>> nodesBySize = new HashMap<>();

    private final Map<Integer, UsageReport> usageReport = new HashMap<>();

    private final SampleStore sampleStore;

    private final Timer createDiffNodeTimer;

    private final Timer compareDiffNodesTimer;

    public DiffBasedFilter(SampleStore sampleStore, MeterRegistry registry) {
        this.sampleStore = sampleStore;
        this.createDiffNodeTimer =
                registry.timer(MeterNames.FILTER_DIFF_CREATE);
        this.compareDiffNodesTimer =
                registry.timer(MeterNames.RDF4J_MODEL_ISOMORPHIC);
    }

    @Override
    public boolean discoveryWillRun(Discovery context) {
        Node root = context.getRoot();
        try {
            this.root = new HashSet<>(
                    sampleStore.load(root.getDataSampleRef()));
        } catch (DiscoveryException ex) {
            LOG.error("Can't save root data sample.", ex);
            return false;
        }
        return true;
    }

    @Override
    public boolean nodeDidExpand(Node node) {
        if (node.isRedundant()) {
            return true;
        }
        DiffRef diff;
        try {
            diff = createNodeDiffRef(node);
        } catch (DiscoveryException ex) {
            LOG.error("Can't create node diff.", ex);
            return false;
        }
        Integer size = diff.size;
        if (!nodesBySize.containsKey(size)) {
            nodesBySize.put(size, new ArrayList<>());
            usageReport.put(size, new UsageReport());
        }
        nodesBySize.get(size).add(diff);
        return true;
    }

    /// nodeSAMPLE --> NULL
    private DiffRef createNodeDiffRef(Node node) throws DiscoveryException {
        List<Statement> nodeSample = sampleStore.load(node.getDataSampleRef());
        DataSampleDiff diff = createNodeDiff(nodeSample);
        return new DiffRef(
                sampleStore.store(diff.added, SampleGroup.FILTER),
                sampleStore.store(diff.removed, SampleGroup.FILTER),
                diff.size());
    }

    private DataSampleDiff createNodeDiff(List<Statement> nodeSample) {
        Instant start = Instant.now();
        DataSampleDiff diff = DataSampleDiff.create(root, nodeSample);
        createDiffNodeTimer.record(Duration.between(start, Instant.now()));
        return diff;
    }

    @Override
    public boolean isNewNode(Node node, List<Statement> dataSample)
            throws DiscoveryException {
        DataSampleDiff diff = createNodeDiff(dataSample);
        UsageReport report = usageReport.get(diff.size());
        for (var visitedRef : getForSize(diff.size())) {
            DataSampleDiff visitedDiff = resolve(visitedRef);
            if (match(diff, visitedDiff, report)) {
                return false;
            }
        }
        return true;
    }

    private List<DiffRef> getForSize(Integer size) {
        return nodesBySize.getOrDefault(size, Collections.emptyList());
    }

    private DataSampleDiff resolve(DiffRef ref) throws DiscoveryException {
        return DataSampleDiff.fromDiff(
                sampleStore.load(ref.added),
                sampleStore.load(ref.removed));
    }

    private boolean match(
            DataSampleDiff left, DataSampleDiff right, UsageReport report) {
        Instant start = Instant.now();
        boolean result = left.match(right);
        Duration duration = Duration.between(start, Instant.now());
        compareDiffNodesTimer.record(duration);
        // Add information to report.
        report.used += 1;
        report.duration += duration.toMillis();
        if (result) {
            report.match += 1;
        }
        return result;
    }

    @Override
    public boolean levelDidEnd(int level) {
        StringBuilder message = new StringBuilder(
                "For given size number of data samples:");
        List<Integer> sizes = new ArrayList<>(nodesBySize.keySet());
        Collections.sort(sizes);
        for (Integer size : sizes) {
            UsageReport report = usageReport.get(size);
            message.append("\n    size: ")
                    .append(size)
                    .append(" count: ")
                    .append(nodesBySize.get(size).size())
                    .append(" used: ")
                    .append(report.used)
                    .append(" matched: ")
                    .append(report.match)
                    .append(" duration: ")
                    .append(report.duration)
                    .append(" ms");
        }
        LOG.debug(message.toString());
        return true;
    }

    @Override
    public void save(File directory, SampleRefToString sampleRefToString)
            throws IOException {
        List<String> added = new ArrayList<>();
        List<String> removed = new ArrayList<>();
        List<Integer> sizes = new ArrayList<>();
        for (var entry : nodesBySize.entrySet()) {
            for (DiffRef diffRef : entry.getValue()) {
                added.add(sampleRefToString.convert(diffRef.added));
                removed.add(sampleRefToString.convert(diffRef.removed));
                sizes.add(diffRef.size);
            }
        }
        IoContainer container = new IoContainer();
        container.added = added;
        container.removed = removed;
        container.sizes = sizes;
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(getDataFile(directory), container);
    }

    private File getDataFile(File directory) {
        return new File(directory, "rdf4j-isomorphic-diff-filter.json");
    }


    @Override
    public void load(File directory, StringToSampleRef stringToSampleRef)
            throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        IoContainer container = objectMapper.readValue(
                getDataFile(directory),
                IoContainer.class);
        for (int index = 0; index < container.added.size(); ++index) {
            DiffRef diffRef = new DiffRef(
                    stringToSampleRef.convert(container.added.get(index)),
                    stringToSampleRef.convert(container.removed.get(index)),
                    container.sizes.get(index)
            );
            if (!nodesBySize.containsKey(diffRef.size)) {
                nodesBySize.put(diffRef.size, new ArrayList<>());
                usageReport.put(diffRef.size, new UsageReport());
            }
            nodesBySize.get(diffRef.size).add(diffRef);
        }
    }

}
