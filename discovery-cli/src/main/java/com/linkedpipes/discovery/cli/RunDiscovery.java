package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.DiscoveryRunner;
import com.linkedpipes.discovery.statistics.Statistics;
import com.linkedpipes.discovery.statistics.StatisticsAdapter;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.cli.export.DataSamplesExport;
import com.linkedpipes.discovery.cli.export.GephiExport;
import com.linkedpipes.discovery.cli.export.JsonPipelineExport;
import com.linkedpipes.discovery.cli.factory.BuilderConfiguration;
import com.linkedpipes.discovery.cli.factory.DiscoveriesFromUrl;
import com.linkedpipes.discovery.io.DiscoveryAdapter;
import com.linkedpipes.discovery.statistics.CollectStatistics;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.node.ShakeNonExpandedNodes;
import com.linkedpipes.discovery.node.ShakeRedundantNodes;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

class RunDiscovery {

    @FunctionalInterface
    interface ResultsHandler {

        void apply(
                Discovery discovery, Dataset dataset,
                Statistics statistics, String name);

    }

    private static final Logger LOG =
            LoggerFactory.getLogger(RunDiscovery.class);

    private final BuilderConfiguration configuration;

    private ResultsHandler resultsHandler;

    public RunDiscovery(BuilderConfiguration configuration) {
        this.configuration = configuration;
    }

    public void run(String definitionUrl, ResultsHandler resultsHandler)
            throws Exception {
        this.resultsHandler = resultsHandler;
        MeterRegistry registry = createMeterRegistry();
        Instant start = Instant.now();
        runDiscoveriesFromUrl(definitionUrl, registry);
        LOG.info("Running {} takes {} min",
                definitionUrl,
                Duration.between(start, Instant.now()).toMinutes());
    }

    private MeterRegistry createMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    private void runDiscoveriesFromUrl(
            String discoveryUrl, MeterRegistry registry) throws Exception {
        DiscoveriesFromUrl discoveriesFromUrl =
                new DiscoveriesFromUrl(discoveryUrl);
        discoveriesFromUrl.create(configuration, registry,
                ((name, directory, dataset, resumed, discovery) -> {
                    if (isFinished(directory)) {
                        loadFinishedExecution(
                                discovery, dataset, directory, name);
                    } else {
                        runDiscovery(
                                name, directory, dataset, discovery,
                                resumed, registry);
                    }
                }));
    }

    private boolean isFinished(File directory) {
        return configuration.resume
                && StatisticsAdapter.statisticsSaved(directory);
    }

    private void loadFinishedExecution(
            Discovery discovery, Dataset dataset, File directory, String name)
            throws DiscoveryException {
        StatisticsAdapter statisticsAdapter =
                new StatisticsAdapter();
        Statistics statistics = statisticsAdapter.load(discovery, directory);
        onDiscoveryFinished(discovery, dataset, statistics, name);
    }

    private void onDiscoveryFinished(
            Discovery discovery, Dataset dataset,
            Statistics statistics, String name) {
        resultsHandler.apply(discovery, dataset, statistics, name);
    }

    private void runDiscovery(
            String name, File directory, Dataset dataset,
            Discovery discovery, boolean resumed, MeterRegistry registry)
            throws DiscoveryException {
        LOG.info("Exploring dataset: {}", dataset.iri);
        StatisticsAdapter statisticsAdapter = new StatisticsAdapter();
        CollectStatistics collectStatistics = new CollectStatistics();
        if (resumed) {
            // As we resumed the discovery, we need to load statistics.
            collectStatistics.resume(
                    statisticsAdapter.load(discovery, directory));
        }
        discovery.addListener(collectStatistics);
        DiscoveryRunner discoveryRunner = new DiscoveryRunner();
        discoveryRunner.explore(discovery);
        // Save resume data if we have not searched all.
        DiscoveryAdapter discoveryAdapter = new DiscoveryAdapter();
        if (!discovery.getQueue().isEmpty()) {
            LOG.info("Saving resume data");
            discoveryAdapter.saveForResume(discovery, directory);
        } else {
            LOG.info("Saving exploration results");
            discoveryAdapter.saveFinishedDiscovery(discovery, directory);
            statisticsAdapter.save(
                    collectStatistics.getStatistics(), directory);
        }
        Node root = discovery.getRoot();
        LOG.info("Shaking discovery tree");
        (new ShakeNonExpandedNodes()).shake(root);
        (new ShakeRedundantNodes()).shake(root);
        try {
            LOG.info("Exporting ...");
            export(discovery, dataset, root, directory);
        } catch (IOException ex) {
            throw new DiscoveryException(
                    "Export failed for: {}", name, ex);
        }
        onDiscoveryFinished(
                discovery, dataset,
                collectStatistics.getStatistics(), name);
        discovery.cleanUp();
        logMeterRegistry(registry);
    }

    private void export(
            Discovery discovery, Dataset dataset, Node root, File output)
            throws IOException {
        GephiExport.export(root,
                new File(output, "gephi-edges.csv"),
                new File(output, "gephi-vertices.csv"),
                discovery.getApplications(),
                discovery.getGroups());
        JsonPipelineExport.export(
                discovery, dataset, new File(output, "pipelines.json"));
        DataSamplesExport.export(
                root, discovery.getStore(),
                new File(output, "node-data-samples"));
    }

    public void logMeterRegistry(MeterRegistry registry) {
        String message = "Runtime statistics:" + System.lineSeparator()
                + "    store.file.io     :  %8d s" + System.lineSeparator()
                + "    repository create :  %8d s" + System.lineSeparator()
                + "    repository update :  %8d s" + System.lineSeparator()
                + "    repository ask    :  %8d s" + System.lineSeparator()
                + "    rdf4j.isomorphic  :  %8d s" + System.lineSeparator()
                + "    filter.diff.create:  %8d s" + System.lineSeparator()
                + "    store.map         :  %8d s" + System.lineSeparator()
                + "    store.breakup.add :  %8d s" + System.lineSeparator()
                + "    store.breakup.get :  %8d s" + System.lineSeparator()
                + "    store.breakup.io  :  %8d s" + System.lineSeparator()
                + "    store.diff.create :  %8d s" + System.lineSeparator();
        LOG.info(String.format(message,
                (int) registry.timer(MeterNames.FILE_STORE_IO)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.CREATE_REPOSITORY)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.UPDATE_DATA)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.MATCH_DATA)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.RDF4J_MODEL_ISOMORPHIC)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.FILTER_DIFF_CREATE)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.STORE_MAP_MEMORY)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.BREAKUP_STORE_ADD)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.BREAKUP_STORE_GET)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.BREAKUP_STORE_IO)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.DIFF_STORE_CONSTRUCT)
                        .totalTime(TimeUnit.SECONDS)));
    }

}
