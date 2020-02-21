package com.linkedpipes.discovery;

import com.linkedpipes.discovery.filter.FilterStrategy;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.ExpandNode;
import com.linkedpipes.discovery.node.Node;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Explore transformations applied to a single data source.
 */
public class Discovery {

    private static final Logger LOG = LoggerFactory.getLogger(Discovery.class);

    private final String iri;

    private final Dataset dataset;

    private final ExpandNode expander;

    private final List<Transformer> transformers;

    private final List<Application> applications;

    private final FilterStrategy filter;

    /**
     * Nodes to visit.
     */
    private final Deque<Node> queue = new ArrayDeque<>();

    private DiscoveryStatistics statistics;

    /**
     * Statistics for current level.
     */
    private DiscoveryStatistics.Level levelStatistics;

    private Instant currentLevelStart;

    public Discovery(
            String iri,
            Dataset dataset,
            List<Transformer> transformers,
            List<Application> applications,
            FilterStrategy filter,
            MeterRegistry registry) {
        this.iri = iri;
        this.dataset = dataset;
        this.transformers = transformers;
        this.applications = applications;
        this.expander = new ExpandNode(applications, transformers, registry);
        this.filter = filter;
    }

    @SuppressFBWarnings(value = {"DM_GC"})
    public Node explore(int levelLimit) {
        // At the start of each exploration cycle we ask for a GC, it may
        // not happen but it may help to get better values for the memory
        // usage.
        Runtime.getRuntime().gc();
        //
        LOG.info("Running exploration for: {}", dataset.iri);
        LOG.info(
                "data sample: {} applications: {} transformers: {} "
                        + "memory: {} MB",
                dataset.sample.size(), applications.size(),
                transformers.size(), usedMemoryInMb());
        initializeStatistics();
        Node root = createRoot();
        initializeExploration(root);
        int currentLevel = root.getLevel();
        while (!queue.isEmpty()) {
            currentLevel = checkStartOfNextLevel(currentLevel);
            if (currentLevel == levelLimit) {
                LOG.info("Level limit ({}) reached!", levelLimit);
                break;
            }
            Node next = queue.pop();
            // TODO If we need to remember all nodes, here is the place.
            expander.expand(next);
            addApplicationsAndTransformersOfExpandedNodeToStatistics(next);
            levelStatistics.generated += next.getNext().size();
            filterNewNodes(next);
            levelStatistics.size += next.getNext().size();
            addNewNodes(next.getNext());
        }
        finalizeStatisticsForCurrentLevel();
        logCurrentLevelInfo();
        return root;
    }

    private long usedMemoryInMb() {
        Runtime runtime = Runtime.getRuntime();
        long mb = 1024 * 1024;
        return (runtime.totalMemory() - runtime.freeMemory()) / mb;
    }

    private void initializeStatistics() {
        statistics = new DiscoveryStatistics();
        statistics.discoveryIri = iri;
        statistics.dataset = new DiscoveryStatistics.DatasetRef(dataset);
        levelStatistics = new DiscoveryStatistics.Level();
        statistics.levels.add(levelStatistics);
        currentLevelStart = Instant.now();
    }

    private Node createRoot() {
        return new Node(Collections.singletonList(dataset));
    }

    private void initializeExploration(Node root) {
        filter.init(root);
        queue.add(root);
    }

    private int checkStartOfNextLevel(int currentLevel) {
        Node next = queue.peek();
        if (next.getLevel() > currentLevel) {
            onNextLevelStart();
        }
        return next.getLevel();
    }

    private void onNextLevelStart() {
        finalizeStatisticsForCurrentLevel();
        currentLevelStart = Instant.now();
        logCurrentLevelInfo();
        // Add new level of statistics.
        int levelIndex = levelStatistics.level + 1;
        levelStatistics = new DiscoveryStatistics.Level();
        levelStatistics.level = levelIndex;
        statistics.levels.add(levelStatistics);
    }

    private void finalizeStatisticsForCurrentLevel() {
        Instant now = Instant.now();
        levelStatistics.meters.put(
                MeterNames.TOTAL_TIME,
                Duration.between(currentLevelStart, now).getSeconds());
    }


    private void logCurrentLevelInfo() {
        int nemApplicationCount = 0;
        for (Application application : levelStatistics.applications) {
            for (DiscoveryStatistics.Level level : statistics.levels) {
                if (level == levelStatistics) {
                    // We made it to current level and we have not
                    // seen the app so far, it must be a new application.
                    nemApplicationCount += 1;
                    break;
                }
                if (level.applications.contains(application)) {
                    break;
                }
            }
        }
        LOG.info(
                " level: {} "
                        + "generated nodes: {} new nodes: {} new apps: {} "
                        + "apps: {} transformers: {} "
                        + "memory: {} MB",
                levelStatistics.level,
                levelStatistics.generated,
                levelStatistics.size,
                nemApplicationCount,
                levelStatistics.applications.size(),
                levelStatistics.transformers.size(),
                usedMemoryInMb());
    }

    private void addApplicationsAndTransformersOfExpandedNodeToStatistics(
            Node node) {
        levelStatistics.applications.addAll(node.getApplications());
        levelStatistics.transformers.add(node.getTransformer());
        node.getApplications().forEach(application -> {
            int value =
                    levelStatistics.pipelinesPerApplication
                            .getOrDefault(application, 0) + 1;
            levelStatistics.pipelinesPerApplication.put(application, value);
        });
    }

    /**
     * Filter out already explored (visited or waiting to be visited) nodes.
     * So we do not visit the same state multiple times.
     */
    private void filterNewNodes(Node node) {
        List<Node> filtered = node.getNext().stream()
                .filter(filter::isNewNode)
                .collect(Collectors.toList());
        node.setNext(filtered);
    }

    private void addNewNodes(List<Node> newNodes) {
        queue.addAll(newNodes);
        newNodes.forEach(filter::addNode);
    }

    public String getIri() {
        return iri;
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

    public DiscoveryStatistics getStatistics() {
        return statistics;
    }

}
