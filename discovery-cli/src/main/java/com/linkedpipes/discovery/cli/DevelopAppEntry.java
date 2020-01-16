package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.cli.factory.FromFileSystem;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

    private void runOnAllDatasets() throws Exception {
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

    private void runExperiment(String datasetDir) throws Exception {
        runExperiment(new File(datasetDir));
    }

    private void runExperiment(File datasetDir)
            throws Exception {
        LOG.info("Running with dataset: {}", datasetDir.getName());
        //
        FromFileSystem builder = new FromFileSystem(datasetDir);
        builder.addApplications(new File("./../data/application"));
        builder.addTransformers(new File("./../data/transformer"));
        File outputDir = new File("./../data/output/" + datasetDir.getName());
        AppEntry.runDiscovery(builder, SEARCH_LIMIT, outputDir);
    }

    private MeterRegistry createMeterRegistry() {
        SimpleMeterRegistry memoryRegistry = new SimpleMeterRegistry();
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        registry.add(memoryRegistry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        return registry;
    }

}
