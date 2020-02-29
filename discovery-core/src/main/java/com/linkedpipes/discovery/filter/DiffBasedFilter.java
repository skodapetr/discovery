package com.linkedpipes.discovery.filter;

import com.google.common.collect.Sets;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.SampleRef;
import com.linkedpipes.discovery.sample.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Use isomorphism on diff from root.
 *
 * <p>Times for https---nkod.opendata.cz-sparql 00:21
 * Exploration statistics:
 * - generated         : 1024
 * - output tree size  : 256
 * Runtime statistics:
 * - filter.diff.create total: 0 s
 * - filter.diff.compare total: 9 s
 *
 * <p>Times for http---data.open.ac.uk-query 03:26
 * Exploration statistics:
 * - generated         : 2816
 * - output tree size  : 512
 * Runtime statistics:
 * - repository.create total: 21 s
 * - filter.diff.create total: 1 s
 * - filter.diff.compare total: 134 s
 */
public class DiffBasedFilter implements NodeFilter {

    private static class UsageReport {

        int used = 0;

        int match = 0;

        long duration = 0;

    }

    private static final Logger LOG =
            LoggerFactory.getLogger(DiffBasedFilter.class);

    /**
     * We keep the root sample in main memory as we use it very often.
     */
    private Set<Statement> rootSample;

    /**
     * Store NodeDiffs in lists by size.
     */
    private final Map<Integer, List<SampleRef>> nodesBySize = new HashMap<>();

    private final Map<Integer, UsageReport> usageReport = new HashMap<>();

    private final SampleStore sampleStore;

    private final Timer createDiffNodeTimer;

    private final Timer compareDiffNodesTimer;

    public DiffBasedFilter(SampleStore sampleStore, MeterRegistry registry) {
        this.sampleStore = sampleStore;
        this.createDiffNodeTimer =
                registry.timer(MeterNames.FILTER_DIFF_CREATE);
        this.compareDiffNodesTimer =
                registry.timer(MeterNames.FILTER_DIFF_FILTER);
    }

    @Override
    public void init(Node root) throws DiscoveryException {
        this.rootSample =
                new HashSet<>(sampleStore.load(root.getDataSampleRef()));
        this.nodesBySize.clear();
    }

    @Override
    public void addNode(Node node) throws DiscoveryException {
        Set<Statement> diff = createNodeDiff(node);
        Integer size = diff.size();
        if (!nodesBySize.containsKey(size)) {
            nodesBySize.put(size, new ArrayList<>());
            usageReport.put(size, new UsageReport());
        }
        SampleRef ref = sampleStore.store(new ArrayList<>(diff), "filter-diff");
        nodesBySize.get(size).add(ref);
    }

    private Set<Statement> createNodeDiff(Node node) throws DiscoveryException {
        Instant start = Instant.now();
        try {
            Set<Statement> nodeSample =
                    new HashSet<>(sampleStore.load(node.getDataSampleRef()));
            return Sets.difference(rootSample, nodeSample);
        } finally {
            createDiffNodeTimer.record(Duration.between(start, Instant.now()));
        }
    }

    @Override
    public boolean isNewNode(Node node) throws DiscoveryException {
        Set<Statement> diff = createNodeDiff(node);
        Instant start = Instant.now();
        UsageReport report = usageReport.get(diff.size());
        try {
            for (SampleRef visitedRef : getForSize(diff.size())) {
                if (match(diff, sampleStore.load(visitedRef))) {
                    report.match += 1;
                    return false;
                }
            }
            return true;
        } finally {
            Duration duration = Duration.between(start, Instant.now());
            compareDiffNodesTimer.record(duration);
            if (report != null) {
                report.used += 1;
                report.duration += duration.toMillis();
            }
        }
    }

    private List<SampleRef> getForSize(Integer size) {
        return nodesBySize.getOrDefault(size, Collections.emptyList());
    }

    private boolean match(
            Collection<Statement> left, Collection<Statement> right) {
        return Models.isomorphic(left, right);
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
