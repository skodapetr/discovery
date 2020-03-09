package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DiffStore;
import com.linkedpipes.discovery.sample.SampleRef;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;
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

public class DiffBasedFilterForDiffStore implements NodeFilter {

    private static final Logger LOG =
            LoggerFactory.getLogger(DiffBasedFilterForDiffStore.class);

    /**
     * Store NodeDiffs in lists by size.
     */
    private final Map<Integer, List<DiffStore.Diff>> nodesBySize
            = new HashMap<>();

    private final Map<Integer, DiffBasedFilter.UsageReport> usageReport
            = new HashMap<>();

    private Set<Statement> root;

    /**
     * When we are provided with diff store, we provide it with a little bit
     * of extra information.
     */
    private final DiffStore store;

    private final Timer createDiffNodeTimer;

    private final Timer compareNodesTimer;

    public DiffBasedFilterForDiffStore(
            DiffStore store, MeterRegistry registry) {
        this.store = store;
        this.createDiffNodeTimer =
                registry.timer(MeterNames.FILTER_DIFF_CREATE);
        this.compareNodesTimer =
                registry.timer(MeterNames.RDF4J_MODEL_ISOMORPHIC);
    }

    @Override
    public void init(Node root) {
        this.root = new HashSet<>(store.load(root.getDataSampleRef()));
    }

    @Override
    public void addNode(Node node) {
        DiffStore.Diff diff = store.getDiff(node.getDataSampleRef());
        Integer size = diff.size();
        if (!nodesBySize.containsKey(size)) {
            nodesBySize.put(size, new ArrayList<>());
            usageReport.put(size, new DiffBasedFilter.UsageReport());
        }
        nodesBySize.get(size).add(diff);
    }

    @Override
    public boolean isNewNode(Node node, List<Statement> dataSample) {
        // The node does not have data sample yet, so we create one.
        Instant start = Instant.now();
        DiffStore.Diff diff = DiffStore.Diff.create(root, dataSample);
        createDiffNodeTimer.record(Duration.between(start, Instant.now()));
        DiffBasedFilter.UsageReport report = usageReport.get(diff.size());
        for (DiffStore.Diff visitedDiff : getForSize(diff.size())) {
            if (match(diff, visitedDiff, report)) {
                return false;
            }
        }
        // We can store the diff into storage, for later use.
        SampleRef ref = store.store(diff);
        node.setDataSampleRef(ref);
        return true;
    }

    private List<DiffStore.Diff> getForSize(Integer size) {
        return nodesBySize.getOrDefault(size, Collections.emptyList());
    }

    private boolean match(
            DiffStore.Diff left, DiffStore.Diff right,
            DiffBasedFilter.UsageReport report) {
        Instant start = Instant.now();
        boolean result = DiffBasedFilterForDiffStore.match(left, right);
        Duration duration = Duration.between(start, Instant.now());
        compareNodesTimer.record(duration);
        // Add information to report.
        report.used += 1;
        report.duration += duration.toMillis();
        if (result) {
            report.match += 1;
        }
        return result;
    }

    public static boolean match(DiffStore.Diff left, DiffStore.Diff right) {
        return left.added.size() == right.added.size()
                && Models.isomorphic(left.added, right.added)
                && Models.isomorphic(left.removed, right.removed);
    }

    @Override
    public void logAfterLevelFinished() {
        StringBuilder message = new StringBuilder(
                "For given size number of data samples:");
        List<Integer> sizes = new ArrayList<>(nodesBySize.keySet());
        Collections.sort(sizes);
        for (Integer size : sizes) {
            DiffBasedFilter.UsageReport report = usageReport.get(size);
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
