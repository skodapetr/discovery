package com.linkedpipes.discovery.filter;

import com.google.common.collect.Sets;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;

import java.util.ArrayList;
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
public class DiffBasedFilter implements FilterStrategy {

    private static class NodeDiff {

        private final Set<Statement> diff;

        public NodeDiff(Set<Statement> diff) {
            this.diff = diff;
        }
    }

    private Set<Statement> rootSample;

    /**
     * Store NodeDiffs in lists by size.
     */
    private final Map<Integer, List<NodeDiff>> nodesBySize = new HashMap<>();

    private final Timer createDiffNodeTimer;

    private final Timer compareDiffNodesTimer;

    public DiffBasedFilter(MeterRegistry registry) {
        this.createDiffNodeTimer =
                registry.timer(MeterNames.FILTER_DIFF_CREATE);
        this.compareDiffNodesTimer =
                registry.timer(MeterNames.FILTER_DIFF_FILTER);
    }

    @Override
    public void init(Node root) {
        this.rootSample = new HashSet<>(root.getDataSample());
        this.nodesBySize.clear();
    }

    @Override
    public void addNode(Node node) {
        NodeDiff nodeDiff = createNodeDiff(node);
        Integer size = nodeDiff.diff.size();
        if (!nodesBySize.containsKey(size)) {
            nodesBySize.put(size, new ArrayList<>());
        }
        nodesBySize.get(size).add(nodeDiff);
    }

    private NodeDiff createNodeDiff(Node node) {
        return createDiffNodeTimer.record(() -> {
            Set<Statement> nodeSample = new HashSet<>(node.getDataSample());
            Set<Statement> diff = Sets.difference(rootSample, nodeSample);
            return new NodeDiff(diff);
        });
    }

    @Override
    public boolean isNewNode(Node node) {
        return compareDiffNodesTimer.record(() -> {
            NodeDiff nodeDiff = createNodeDiff(node);
            Integer size = nodeDiff.diff.size();
            for (NodeDiff visited : getForSize(size)) {
                if (match(visited, nodeDiff)) {
                    return false;
                }
            }
            return true;
        });
    }

    private List<NodeDiff> getForSize(Integer size) {
        return nodesBySize.getOrDefault(size, Collections.emptyList());
    }

    private boolean match(NodeDiff left, NodeDiff right) {
        return Models.isomorphic(left.diff, right.diff);
    }

}
