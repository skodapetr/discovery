package com.linkedpipes.discovery;

import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.AskNode;
import com.linkedpipes.discovery.node.ExpandNode;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.SampleRef;
import com.linkedpipes.discovery.sample.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * Explore transformations applied to a single data source.
 */
public class Discovery {

    private static final Logger LOG = LoggerFactory.getLogger(Discovery.class);

    private static final int MB = 1024 * 1024;

    private final String iri;

    private final Dataset dataset;

    /**
     * Nodes to visit.
     */
    private final Deque<Node> queue = new ArrayDeque<>();

    private DiscoveryStatistics statistics;

    /**
     * Statistics for current level.
     */
    private DiscoveryStatistics.Level levelStatistics;

    private SampleStore store;

    private final NodeFilter filter;

    private final ExpandNode expander;

    private int maxNodeExpansionTimeMs;

    /**
     * Discovery does not end after first exceeding of
     * {@link #maxNodeExpansionTimeMs}, os it may just be saving memory to disk
     * instead we stop discovery after second case.
     */
    private int nodeExpansionTimeExceeded = 0;

    private final Timer discoveryTimer;

    private final List<Application> applications;

    private final List<Transformer> transformers;

    private final DataSampleTransformer dataSampleTransformer;

    public Discovery(
            String iri, Dataset dataset,
            List<Transformer> transformers, List<Application> applications,
            NodeFilter filter, SampleStore store,
            int maxNodeExpansionTimeSeconds,
            DataSampleTransformer dataSampleTransformer,
            MeterRegistry registry) {
        this.iri = iri;
        this.dataset = dataset;
        this.store = store;
        this.maxNodeExpansionTimeMs = maxNodeExpansionTimeSeconds * 1000;
        this.discoveryTimer = registry.timer(MeterNames.DISCOVERY_TIME);
        this.filter = filter;
        AskNode askNode = new AskNode(applications, transformers, registry);
        this.expander = new ExpandNode(
                store, filter, askNode, dataSampleTransformer, registry);
        this.applications = applications;
        this.transformers = transformers;
        this.dataSampleTransformer = dataSampleTransformer;
        LOG.info("Creating exploration for: {}", dataset.iri);
        LOG.info("Data sample size: {} apps: {} transformers: {}",
                dataset.sample.size(), applications.size(),
                transformers.size());
    }

    public Node explore(int levelLimit) throws DiscoveryException {
        LOG.info("Running exploration for: {}", dataset.iri);
        initializeStatistics();
        Node root = initializeExploration();
        Instant nextLogTime = Instant.now().plus(15, ChronoUnit.MINUTES);
        while (!queue.isEmpty()) {
            Node node = queue.pop();
            if (startingNextLevel(node)) {
                if (levelStatistics.level == levelLimit) {
                    LOG.info("Level limit ({}) reached!", levelLimit);
                    break;
                }
                finalizeStatisticsForCurrentLevel();
                logCurrentLevelStatistics();
                addNewLevelStatistics();
                // Update next log time as we reached another level.
                nextLogTime = Instant.now().plus(15, ChronoUnit.MINUTES);
            }
            Instant start = Instant.now();
            try {
                expander.expand(node);
            } catch (OutOfMemoryError ex) {
                LOG.info("Out of memory!");
                break;
            }
            node.setExpanded(true);
            long durationMs = Duration.between(start, Instant.now()).toMillis();
            if (durationMs > maxNodeExpansionTimeMs) {
                nodeExpansionTimeExceeded += 1;
                if (nodeExpansionTimeExceeded >= 2) {
                    LOG.info("Node expansion takes to long ({} s)!",
                            durationMs / 1000);
                    break;
                }
            } else {
                nodeExpansionTimeExceeded = 0;
            }
            // As the computation can take quite a long time we log
            // one upon a time.
            if (Instant.now().isAfter(nextLogTime)) {
                nextLogTime = Instant.now().plus(15, ChronoUnit.MINUTES);
                logProgress(durationMs);
            }
            // Update statistics.
            levelStatistics.expandedNodes += 1;
            if (node.isRedundant()) {
                levelStatistics.filteredNodes += 1;
            } else {
                levelStatistics.newNodes += 1;
                addNode(node);
            }
            // In any case we do not need the data in memory any more.
            // But for redundant nodes the ref may not be available.
            if (node.getDataSampleRef() != null) {
                store.releaseFromMemory(node.getDataSampleRef());
            }
        }
        finalizeStatisticsForCurrentLevel();
        logCurrentLevelStatistics();
        LOG.info("Exploration finished.");
        return root;
    }

    private void initializeStatistics() {
        statistics = new DiscoveryStatistics();
        statistics.discoveryIri = iri;
        statistics.dataset = new DiscoveryStatistics.DatasetRef(dataset);
        levelStatistics = new DiscoveryStatistics.Level();
        levelStatistics.level = 0;
        levelStatistics.startNodes = 1; // We start with just the root.
        statistics.levels.add(levelStatistics);
    }


    private Node initializeExploration() throws DiscoveryException {
        List<Statement> dataSample =
                dataSampleTransformer.transform(dataset.sample);
        SampleRef ref = store.storeRoot(dataSample);
        Node root = new Node(Collections.singletonList(dataset));
        root.setDataSampleRef(ref);
        // We do not add root to the filter as we call init on the filter.
        filter.init(root);
        expander.expandWithDataSample(root);
        root.setExpanded(true);
        queue.addAll(root.getNext());
        levelStatistics.nextLevel = root.getNext().size();
        levelStatistics.expandedNodes += 1;
        return root;
    }

    private void logProgress(long lastExpansionTimeSeconds) {
        Runtime runtime = Runtime.getRuntime();
        LOG.info(
                "           filtered: {} new: {} next level: {} "
                        + "used {} MB allocated: {} MB "
                        + "last expansion: time {} ms",
                levelStatistics.filteredNodes,
                levelStatistics.newNodes,
                levelStatistics.nextLevel,
                (runtime.totalMemory() - runtime.freeMemory()) / MB,
                runtime.totalMemory() / MB,
                lastExpansionTimeSeconds);
    }

    private boolean startingNextLevel(Node node) {
        return node.getLevel() > levelStatistics.level;
    }

    private void finalizeStatisticsForCurrentLevel() {
        levelStatistics.end = Instant.now();
        discoveryTimer.record(Duration.between(
                levelStatistics.start, levelStatistics.end));
    }

    private void logCurrentLevelStatistics() {
        logCurrentLevelInfo();
        store.logAfterLevelFinished();
        filter.logAfterLevelFinished();
        dataSampleTransformer.logAfterLevelFinished();
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
        Runtime runtime = Runtime.getRuntime();
        LOG.info(
                " level: {} filtered: {} new: {} "
                        + "next level: {} new apps: {} "
                        + "apps: {} transformers: {} "
                        + "used: {} MB allocated: {} MB",
                levelStatistics.level,
                levelStatistics.filteredNodes,
                levelStatistics.newNodes,
                levelStatistics.nextLevel,
                nemApplicationCount,
                levelStatistics.applications.size(),
                levelStatistics.transformers.size(),
                (runtime.totalMemory() - runtime.freeMemory()) / MB,
                runtime.totalMemory() / MB);
    }

    private void addNewLevelStatistics() {
        int levelIndex = levelStatistics.level + 1;
        levelStatistics = new DiscoveryStatistics.Level();
        levelStatistics.level = levelIndex;
        // We know that this is a start of a new level,
        // so all in the queue is content of this level.
        levelStatistics.startNodes = queue.size() + 1;
        statistics.levels.add(levelStatistics);
    }

    private void addNode(Node node) throws DiscoveryException {
        levelStatistics.nextLevel += node.getNext().size();
        levelStatistics.applications.addAll(node.getApplications());
        // Root have a null transformer.
        if (node.getTransformer() != null) {
            levelStatistics.transformers.add(node.getTransformer());
        }
        node.getApplications().forEach(application -> {
            int value = levelStatistics.pipelinesPerApplication
                    .getOrDefault(application, 0) + 1;
            levelStatistics.pipelinesPerApplication.put(application, value);
        });
        // Add to next.
        queue.addAll(node.getNext());
        // Add to filter.
        filter.addNode(node);
    }


    public String getIri() {
        return iri;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public DiscoveryStatistics getStatistics() {
        return statistics;
    }

    public SampleStore getStore() {
        return store;
    }

    public void cleanUp() {
        store.cleanUp();
    }

    public List<Application> getApplications() {
        return applications;
    }

    public List<Transformer> getTransformers() {
        return transformers;
    }

}
