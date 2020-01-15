package com.linkedpipes.discovery;

import com.linkedpipes.discovery.filter.FilterStrategy;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.ExpandNode;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.rdf.ExplorerStatistics;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

public class Discovery {

    private static final Logger LOG = LoggerFactory.getLogger(Discovery.class);

    private final Dataset dataset;

    private final ExpandNode expander;

    private final List<Transformer> transformers;

    private final List<Application> applications;

    private final FilterStrategy filter;

    /**
     * Explored nodes.
     */
    private final List<Node> nodes = new ArrayList<>();

    /**
     * Nodes to visit.
     */
    private final Deque<Node> queue = new ArrayDeque<>();

    private ExplorerStatistics statistics;

    public Discovery(
            Dataset dataset,
            List<Transformer> transformers,
            List<Application> applications,
            FilterStrategy filter,
            MeterRegistry registry) {
        this.dataset = dataset;
        this.transformers = transformers;
        this.applications = applications;
        this.expander = new ExpandNode(applications, transformers, registry);
        this.filter = filter;
    }

    public Node explore(int levelLimit) {
        statistics = new ExplorerStatistics();
        Node root = createRoot();
        filter.init(root);
        nodes.add(root);
        queue.add(root);
        while (!queue.isEmpty()) {
            Node next = queue.pop();
            if (next.getLevel() == levelLimit) {
                LOG.info("Level limit ({}) reached!", levelLimit);
                break;
            }
            expander.expand(next);
            int newNodesCount = next.getNext().size();
            statistics.generated += newNodesCount;
            filterNewNodes(next);
            LOG.info(
                    " level: {} # apps: {} # nodes: {} # new nodes: {}",
                    next.getLevel(), next.getApplications().size(),
                    newNodesCount, next.getNext().size());
            statistics.filteredOut = newNodesCount - next.getNext().size();
            addNewNodes(next.getNext());
        }
        statistics.finalSize = nodes.size();
        return root;
    }

    private Node createRoot() {
        return new Node(Collections.singletonList(dataset));
    }

    /**
     * Filter out already explored (visited or waiting to be visited) nodes.
     * So we do not visit the same state multiple times.
     */
    private void filterNewNodes(Node node) {
        List<Node> filtered = node.getNext().stream()
                .filter((newNode) -> filter.isNewNode(newNode))
                .collect(Collectors.toList());
        node.setNext(filtered);
    }

    private void addNewNodes(List<Node> newNodes) {
        queue.addAll(newNodes);
        nodes.addAll(newNodes);
        newNodes.forEach((node) -> filter.addNode(node));
    }

    public Dataset getDataset() {
        return dataset;
    }

    public List<Transformer> getTransformers() {
        return Collections.unmodifiableList(transformers);
    }

    public List<Application> getApplications() {
        return Collections.unmodifiableList(applications);
    }

    public ExplorerStatistics getStatistics() {
        return statistics;
    }

}
