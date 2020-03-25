package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.DiscoveryRunner;
import com.linkedpipes.discovery.statistics.DiscoveryStatisticsAdapter;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.cli.export.DataSamplesExport;
import com.linkedpipes.discovery.cli.export.GephiExport;
import com.linkedpipes.discovery.cli.export.JsonPipelineExport;
import com.linkedpipes.discovery.cli.export.NodeToName;
import com.linkedpipes.discovery.cli.export.SummaryExport;
import com.linkedpipes.discovery.cli.factory.BuilderConfiguration;
import com.linkedpipes.discovery.cli.factory.DiscoveriesFromUrl;
import com.linkedpipes.discovery.cli.model.NamedDiscoveryStatistics;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

class RunDiscovery {

    private static final Logger LOG =
            LoggerFactory.getLogger(RunDiscovery.class);

    private final BuilderConfiguration configuration;

    public RunDiscovery(BuilderConfiguration configuration) {
        this.configuration = configuration;
    }

    public List<NamedDiscoveryStatistics> run(String dataset)
            throws Exception {
        MeterRegistry registry = createMeterRegistry();
        Instant start = Instant.now();
        var result = runDiscoveriesFromUrl(dataset, registry);
        LOG.info("All done in: {} min",
                Duration.between(start, Instant.now()).toMinutes());
        return result;
    }

    private MeterRegistry createMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    private List<NamedDiscoveryStatistics> runDiscoveriesFromUrl(
            String discoveryUrl, MeterRegistry registry) throws Exception {
        DiscoveriesFromUrl discoveriesFromUrl = new DiscoveriesFromUrl(
                configuration, discoveryUrl);
        List<NamedDiscoveryStatistics> result = new ArrayList<>();
        discoveriesFromUrl.create(registry,
                ((name, directory, dataset, discoveryContext) -> {
                    NamedDiscoveryStatistics stat = runDiscovery(
                            name, directory, dataset, discoveryContext,
                            registry);
                    result.add(stat);
                }));
        return result;
    }

    private NamedDiscoveryStatistics runDiscovery(
            String name, File directory, Dataset dataset,
            Discovery discovery, MeterRegistry registry)
            throws DiscoveryException {
        DiscoveryStatisticsAdapter statisticsAdapter =
                new DiscoveryStatisticsAdapter();
        if (configuration.resume
                && statisticsAdapter.statisticsSaved(directory)) {
            // The execution has already been finished, we just load the
            // statistics.
            var statistics = statisticsAdapter.load(discovery, directory);
            return new NamedDiscoveryStatistics(statistics, name);
        }
        //
        CollectStatistics collectStatistics =
                new CollectStatistics(dataset);
        discovery.addListener(collectStatistics);
        DiscoveryRunner discoveryRunner = new DiscoveryRunner();
        LOG.info("Exploring dataset: {}", dataset.iri);
        discoveryRunner.explore(discovery);
        // Save resume data if we have not searched all.
        DiscoveryAdapter discoveryAdapter = new DiscoveryAdapter();
        if (!discovery.getQueue().isEmpty()) {
            LOG.info("Saving resume data.");
            discoveryAdapter.saveForResume(discovery, directory);
        } else {
            LOG.info("Saving exploration results.");
            discoveryAdapter.saveFinishedDiscovery(discovery, directory);
            statisticsAdapter.save(
                    collectStatistics.getStatistics(), directory);
        }
        Node root = discovery.getRoot();
        LOG.info("Shaking discovery tree");
        (new ShakeNonExpandedNodes()).shake(root);
        (new ShakeRedundantNodes()).shake(root);
        var namedStatistics = new NamedDiscoveryStatistics(
                collectStatistics.getStatistics(), name);
        try {
            export(
                    discovery, dataset, root,
                    namedStatistics, directory);
        } catch (IOException ex) {
            throw new DiscoveryException(
                    "Export failed for: {}", name, ex);
        }
        discovery.cleanUp();
        logMeterRegistry(registry);
        return namedStatistics;
    }

    private void export(
            Discovery discovery, Dataset dataset, Node root,
            NamedDiscoveryStatistics statistics, File output)
            throws IOException {
        LOG.info("Exporting ...");
        NodeToName nodeToName = new NodeToName(root);
        GephiExport.export(root,
                new File(output, "gephi-edges.csv"),
                new File(output, "gephi-vertices.csv"),
                nodeToName, discovery.getApplications(),
                discovery.getGroups());
        JsonPipelineExport.export(
                discovery, dataset, root, new File(output, "pipelines.json"),
                nodeToName);
        SummaryExport.export(Collections.singleton(statistics), output);
        DataSamplesExport.export(
                root, nodeToName,
                discovery.getStore(),
                new File(output, "node-data-samples"));
    }

    public void logMeterRegistry(MeterRegistry registry) {
        String message = "Runtime statistics:" + System.lineSeparator()
                + "    total time        :  %8d s" + System.lineSeparator()
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
                (int) registry.timer(MeterNames.DISCOVERY_TIME)
                        .totalTime(TimeUnit.SECONDS),
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
