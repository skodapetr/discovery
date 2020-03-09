package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DiffStore;
import com.linkedpipes.discovery.sample.SampleRef;
import com.linkedpipes.discovery.sample.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void init(Node root) throws DiscoveryException {
        this.root = new HashSet<>(sampleStore.load(root.getDataSampleRef()));
    }

    @Override
    public void addNode(Node node) throws DiscoveryException {
        DiffRef diff = createNodeDiffRef(node);
        Integer size = diff.size;
        if (!nodesBySize.containsKey(size)) {
            nodesBySize.put(size, new ArrayList<>());
            usageReport.put(size, new UsageReport());
        }
        nodesBySize.get(size).add(diff);
    }

    private DiffRef createNodeDiffRef(Node node) throws DiscoveryException {
        List<Statement> nodeSample = sampleStore.load(node.getDataSampleRef());
        DiffStore.Diff diff = createNodeDiff(nodeSample);
        return new DiffRef(
                sampleStore.store(diff.added, REF_NAME + "_added"),
                sampleStore.store(diff.removed, REF_NAME + "_remove"),
                diff.size());
    }

    private DiffStore.Diff createNodeDiff(List<Statement> nodeSample) {
        Instant start = Instant.now();
        DiffStore.Diff diff = DiffStore.Diff.create(root, nodeSample);
        createDiffNodeTimer.record(Duration.between(start, Instant.now()));
        return diff;
    }

    @Override
    public boolean isNewNode(Node node, List<Statement> dataSample)
            throws DiscoveryException {
        DiffStore.Diff diff = createNodeDiff(dataSample);
        UsageReport report = usageReport.get(diff.size());
        for (var visitedRef : getForSize(diff.size())) {
            DiffStore.Diff visitedDiff = resolve(visitedRef);
            if (match(diff, visitedDiff, report)) {
                return false;
            }
        }
        return true;
    }

    private List<DiffRef> getForSize(Integer size) {
        return nodesBySize.getOrDefault(size, Collections.emptyList());
    }

    private DiffStore.Diff resolve(DiffRef ref) throws DiscoveryException {
        return DiffStore.Diff.fromDiff(
                sampleStore.load(ref.added),
                sampleStore.load(ref.removed));
    }

    private boolean match(
            DiffStore.Diff left, DiffStore.Diff right, UsageReport report) {
        Instant start = Instant.now();
        boolean result = DiffBasedFilterForDiffStore.match(left, right);
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
    public void logAfterLevelFinished() {
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
    }

}
