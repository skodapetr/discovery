package com.linkedpipes.discovery.statistics;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryListener;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Collect and log discovery statistics.
 */
public class CollectStatistics implements DiscoveryListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(CollectStatistics.class);

    private static final int MB = 2014 * 1024;

    private final DiscoveryStatistics statistics;

    /**
     * Shortcut for current level.
     */
    private DiscoveryStatistics.Level levelStatistics;

    private Instant nextLogTime = Instant.now().plus(15, ChronoUnit.MINUTES);

    private Instant lastExpansionStart;

    private Instant lastLevelStart;

    private Discovery context;

    public CollectStatistics(Dataset dataset) {
        this.statistics = new DiscoveryStatistics();
        statistics.dataset = new DiscoveryStatistics.DatasetRef(dataset);
    }

    @Override
    public boolean discoveryWillRun(Discovery context) {
        int level = 0;
        Node node = context.getQueue().peek();
        if (node != null) {
            level = node.getLevel();
        }
        this.context = context;
        statistics.discoveryIri = context.getIri();
        levelStatistics = new DiscoveryStatistics.Level();
        levelStatistics.level = level;
        levelStatistics.startNodes = 1; // We start with just the root.
        statistics.levels.add(levelStatistics);
        lastLevelStart = Instant.now();
        return true;
    }

    @Override
    public void discoveryDidRun() {

    }

    @Override
    public boolean levelDidEnd(int level) {
        logAfterLevel();
        prepareStatisticsForNewLevel();
        return true;
    }

    private void logAfterLevel() {
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
                countNewApplications(),
                levelStatistics.applications.size(),
                levelStatistics.transformers.size(),
                (runtime.totalMemory() - runtime.freeMemory()) / MB,
                runtime.totalMemory() / MB);
    }

    private int countNewApplications() {
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
        return nemApplicationCount;
    }

    private void prepareStatisticsForNewLevel() {
        levelStatistics.durationInSeconds +=
                Duration.between(lastLevelStart, Instant.now()).toSeconds();
        lastLevelStart = Instant.now();
        int levelIndex = levelStatistics.level + 1;
        levelStatistics = new DiscoveryStatistics.Level();
        levelStatistics.level = levelIndex;
        // We know that this is a start of a new level,
        // so all in the queue is content of this level.
        levelStatistics.startNodes = context.getQueue().size();
        statistics.levels.add(levelStatistics);
    }

    @Override
    public boolean nodeWillExpand(Node node) {
        lastExpansionStart = Instant.now();
        return true;
    }

    @Override
    public boolean nodeDidExpand(Node node) {
        levelStatistics.nextLevel += node.getNext().size();
        levelStatistics.applications.addAll(node.getApplications());
        levelStatistics.expandedNodes += 1;
        if (node.isRedundant()) {
            levelStatistics.filteredNodes += 1;
        } else {
            levelStatistics.newNodes += 1;
        }
        // Root have a null transformer.
        if (node.getTransformer() != null) {
            levelStatistics.transformers.add(node.getTransformer());
        }
        node.getApplications().forEach(application -> {
            int value = levelStatistics.pipelinesPerApplication
                    .getOrDefault(application, 0) + 1;
            levelStatistics.pipelinesPerApplication.put(application, value);
        });
        logStatisticsIfWeShould();
        return true;
    }

    private void logStatisticsIfWeShould() {
        if (Instant.now().isBefore(nextLogTime)) {
            return;
        }
        nextLogTime = Instant.now().plus(15, ChronoUnit.MINUTES);
        long expansionTime =
                Duration.between(lastExpansionStart, Instant.now())
                        .toSeconds();
        logStatistics(expansionTime);
    }

    private void logStatistics(long lastExpansionTimeSeconds) {
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

    public DiscoveryStatistics getStatistics() {
        return statistics;
    }

}
