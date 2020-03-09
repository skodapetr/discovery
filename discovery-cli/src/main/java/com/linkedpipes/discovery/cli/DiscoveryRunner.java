package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.cli.export.DataSamplesExport;
import com.linkedpipes.discovery.cli.export.GephiExport;
import com.linkedpipes.discovery.cli.export.JsonPipelineExport;
import com.linkedpipes.discovery.cli.export.NodeToName;
import com.linkedpipes.discovery.cli.export.SummaryExport;
import com.linkedpipes.discovery.cli.factory.BuilderConfiguration;
import com.linkedpipes.discovery.cli.factory.DiscoveryBuilder;
import com.linkedpipes.discovery.cli.factory.FromDiscoveryUrl;
import com.linkedpipes.discovery.cli.model.DiscoveryStatisticsInPath;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

class DiscoveryRunner {

    private static final Logger LOG =
            LoggerFactory.getLogger(DiscoveryRunner.class);

    private final BuilderConfiguration configuration;

    public DiscoveryRunner(BuilderConfiguration configuration) {
        this.configuration = configuration;
    }

    public List<DiscoveryStatisticsInPath> run(String dataset)
            throws Exception {
        MeterRegistry registry = createMeterRegistry();
        Instant start = Instant.now();
        var result = runDiscovery(dataset, registry);
        LOG.info("All done in: {} min",
                Duration.between(start, Instant.now()).toMinutes());
        return result;
    }

    private MeterRegistry createMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    private List<DiscoveryStatisticsInPath> runDiscovery(
            String dataset, MeterRegistry registry) throws Exception {
        Map<String, String> discoveryNames = new HashMap<>();
        Function<String, String> nameFactory = (iri) -> {
            String name = "discovery_"
                    + String.format("%03d", discoveryNames.size());
            discoveryNames.put(iri, name);
            return name;
        };
        DiscoveryBuilder builder = new FromDiscoveryUrl(
                configuration, nameFactory, dataset);
        List<DiscoveryStatisticsInPath> statistics = new ArrayList<>();
        for (Discovery discovery : builder.create(registry)) {
            Node root = discovery.explore(configuration.limit);
            (new ShakeNonExpandedNodes()).shake(root);
            (new ShakeRedundantNodes()).shake(root);
            String name = discoveryNames.get(discovery.getIri());
            export(discovery, root, new File(configuration.output, name));
            statistics.add(new DiscoveryStatisticsInPath(
                    discovery.getStatistics(), name));
            discovery.cleanUp();
            // Print statistics during the execution.
            logMeterRegistry(registry);
            // TODO Optically delete the working directory here.
        }
        SummaryExport.export(statistics, configuration.output);
        return statistics;
    }

    private void export(Discovery discovery, Node root, File output)
            throws IOException {
        LOG.info("Exporting ...");
        NodeToName nodeToName = new NodeToName(root);
        GephiExport.export(root,
                new File(output, "gephi-edges.csv"),
                new File(output, "gephi-vertices.csv"),
                nodeToName, discovery.getApplications());
        JsonPipelineExport.export(
                discovery, root, new File(output, "pipelines.json"),
                nodeToName);
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
