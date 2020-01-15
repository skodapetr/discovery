package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.cli.export.DataSamplesExport;
import com.linkedpipes.discovery.cli.export.GephiExport;
import com.linkedpipes.discovery.cli.export.NodeToName;
import com.linkedpipes.discovery.cli.factory.FromFileSystem;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.rdf.ExplorerStatistics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Alternative entry point used to execute set of experiments from
 * localhost at once, for debugging and develop purpose.
 */
public class DevelopAppEntry {

    private static final Logger LOG =
            LoggerFactory.getLogger(DevelopAppEntry.class);

    private static final String DATASET_DIR = "./../data/dataset/";

    private static final Integer SEARCH_LIMIT = -1;

    public static void main(String[] args) throws Exception {
        (new AppEntry()).run(args);
    }

    public void run(String[] args) throws Exception {
        // runOnAllDatasets();
        // runExperiment(DATASET_DIR + "http---data.open.ac.uk-query");
        runExperiment(DATASET_DIR + "https---nkod.opendata.cz-sparql");
    }

    private void runOnAllDatasets() throws IOException {
        File datasetRoot = new File(DATASET_DIR);
        File[] datasetDirs = datasetRoot.listFiles();
        if (datasetDirs == null) {
            LOG.error("No dataset directory files found!");
            return;
        }
        Map<String, Long> executionTimes = new HashMap<>();
        for (File datasetDir : datasetDirs) {
            Instant start = Instant.now();
            runExperiment(datasetDir);
            Long duration = Duration.between(start, Instant.now()).getSeconds();
            executionTimes.put(datasetDir.getName(), duration);
        }
        LOG.info("Execution durations:");
        for (var entry : executionTimes.entrySet()) {
            LOG.info("  {} : {} s", entry.getKey(), entry.getValue());
        }
    }

    private void runExperiment(String datasetDir) throws IOException {
        runExperiment(new File(datasetDir));
    }

    private void runExperiment(File datasetDir)
            throws IOException {
        LOG.info("Running with dataset: {}", datasetDir.getName());
        MeterRegistry registry = createMeterRegistry();
        //
        FromFileSystem builder = new FromFileSystem(datasetDir);
        builder.addApplications(new File("./../data/application"));
        builder.addTransformers(new File("./../data/transformer"));
        Discovery discovery = builder.create(registry);
        Node root = discovery.explore(SEARCH_LIMIT);
        logDiscoveryStats(discovery);
        AppEntry.logMeterRegistry(registry);
        LOG.info("Exporting for Gephi ...");
        String name = datasetDir.getName();
        File outputDir = new File("./../data/output/" + name);
        NodeToName nodeToName = new NodeToName(root);
        GephiExport.export(root,
                new File(outputDir, "edges.csv"),
                new File(outputDir, "vertices.csv"),
                nodeToName,
                discovery.getApplications());
        DataSamplesExport.export(
                root, nodeToName, new File(outputDir, "data-samples"));
    }

    private MeterRegistry createMeterRegistry() {
        SimpleMeterRegistry memoryRegistry = new SimpleMeterRegistry();
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        registry.add(memoryRegistry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        return registry;
    }

    private void logDiscoveryStats(Discovery discovery) {
        ExplorerStatistics stats = discovery.getStatistics();
        LOG.info("Exploration statistics:"
                        + "\n    generated         : {}"
                        + "\n    output tree size  : {}",
                stats.generated, stats.finalSize);
    }

}
