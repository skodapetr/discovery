package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.cli.experiment.ExperimentFiles;
import com.linkedpipes.discovery.cli.factory.BuilderConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Entry point for running experiments ~ collections of discoveries.
 */
public class RunExperiment {

    private static final Logger LOG =
            LoggerFactory.getLogger(RunExperiment.class);

    private final BuilderConfiguration configuration;

    public RunExperiment(BuilderConfiguration configuration) {
        this.configuration = configuration;
    }

    public void run(String experimentUrl) throws Exception {
        Instant start = Instant.now();
        Experiment experiment = loadExperiment(experimentUrl);
        mergeToConfiguration(experiment);
        LOG.info("Collected {} discoveries in experiment {}",
                experiment.discoveries.size(), experiment);
        ExperimentFiles experimentFiles = new ExperimentFiles();
        (new File(configuration.output)).mkdirs();
        Map<String, Long> discoveryDurationsInSeconds = new HashMap<>();
        for (int index = 0; index < experiment.discoveries.size(); ++index) {
            Instant discoveryStart = Instant.now();
            String name = String.format("%03d", index);
            BuilderConfiguration discoveryConfig = configuration.copy();
            discoveryConfig.output =
                    Paths.get(configuration.output, name).toString();
            RunDiscovery runner = new RunDiscovery(discoveryConfig);
            runner.run(experiment.discoveries.get(index),
                    (discovery, dataset, statistics, discoveryName) -> {
                        // We update discovery name to reflect
                        // experiment folder.
                        experimentFiles.add(
                                name + "/" + discoveryName,
                                discovery, dataset, statistics);
                    });
            discoveryDurationsInSeconds.put(
                    experiment.discoveries.get(index),
                    Duration.between(discoveryStart, Instant.now())
                            .getSeconds());
        }
        experimentFiles.write(
                new File(configuration.output), discoveryDurationsInSeconds);
        LOG.info("All done in: {} min",
                Duration.between(start, Instant.now()).toMinutes());
    }

    @SuppressFBWarnings(value = {"DM_EXIT"})
    private Experiment loadExperiment(String experimentUrl) {
        try {
            return ExperimentAdapter.load(experimentUrl);
        } catch (IOException ex) {
            LOG.warn("Can't resolve experiment URL: {}", experimentUrl);
            System.exit(1);
            // IDEA fail to detect above as application end.
            throw new RuntimeException();
        }
    }

    public void mergeToConfiguration(Experiment experiment) {
        if (configuration.output == null) {
            configuration.output = experiment.output;
        } else if (experiment.output != null) {
            configuration.output = Paths.get(
                    configuration.output, experiment.output).toString();
        }
    }

}
