package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.cli.export.DataSamplesExport;
import com.linkedpipes.discovery.cli.export.NodeToName;
import com.linkedpipes.discovery.cli.export.JsonPipelineExport;
import com.linkedpipes.discovery.cli.export.SummaryExport;
import com.linkedpipes.discovery.cli.factory.DiscoveryBuilder;
import com.linkedpipes.discovery.cli.factory.FromDiscoveryUrl;
import com.linkedpipes.discovery.cli.factory.FromFileSystem;
import com.linkedpipes.discovery.cli.export.GephiExport;
import com.linkedpipes.discovery.cli.model.DiscoveryStatisticsInPath;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.DiscoveryStatistics;
import com.linkedpipes.discovery.sample.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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

/**
 * Application entry point.
 */
public class AppEntry {

    private static final Logger LOG = LoggerFactory.getLogger(AppEntry.class);

    public static void main(String[] args) throws Exception {
        (new AppEntry()).run(args);
    }

    @SuppressFBWarnings(value = {"DM_EXIT"})
    public void run(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        DiscoveryBuilder builder = null;
        File output = new File(cmd.getOptionValue("output"));
        int limit = Integer.parseInt(cmd.getOptionValue("limit", "-1"));
        if (cmd.hasOption("experiment")) {
            // An experiment is handled by a different class.
            ExperimentRunner runner = new ExperimentRunner(
                    cmd.hasOption("IHaveBadDiscoveryDefinition"),
                    limit);
            runner.run(cmd.getOptionValue("experiment"), output);
            return;
        }
        if (cmd.hasOption("discovery")) {
            if (cmd.hasOption("dataset")) {
                System.out.println(
                        "Options 'discovery' and 'dataset' "
                                + "can not be used together");
                System.exit(1);
            }
            builder = new FromDiscoveryUrl(cmd.getOptionValue("discovery"));
            if (cmd.hasOption("IHaveBadDiscoveryDefinition")) {
                ((FromDiscoveryUrl) builder).setIgnoreIssues(true);
                ((FromDiscoveryUrl) builder).setReport(
                        new File(output, "builder-report.txt"));
            }
        } else if (cmd.hasOption("dataset")) {
            builder = new FromFileSystem(
                    new File(cmd.getOptionValue("dataset")));
        } else {
            System.out.println(
                    "Either 'discovery' or 'dataset' must be set.");
            System.exit(1);
        }
        if (cmd.hasOption("applications")) {
            builder.addApplications(
                    new File(cmd.getOptionValue("applications")));
        }
        if (cmd.hasOption("transformers")) {
            builder.addTransformers(
                    new File(cmd.getOptionValue("transformers")));
        }
        if (cmd.hasOption("filter")) {
            builder.setFilterStrategy(cmd.getOptionValue("filter"));
        }
        MeterRegistry registry = createMeterRegistry();
        Instant start = Instant.now();
        runDiscovery(builder, limit, registry, output);
        logMeterRegistry(registry);
        LOG.info("All done in: {} min",
                Duration.between(start, Instant.now()).toMinutes());
    }

    @SuppressFBWarnings(value = {"DM_EXIT"})
    private CommandLine parseArgs(String[] args) {
        Options options = new Options();

        Option experiment = new Option(
                "e", "experiment", true, "url of a experiment to run");
        experiment.setRequired(false);
        options.addOption(experiment);

        Option discovery = new Option(
                "d", "discovery", true, "url of a discovery to run");
        discovery.setRequired(false);
        options.addOption(discovery);

        Option applications = new Option(
                null, "applications", true, "Path to application directory.");
        applications.setRequired(false);
        options.addOption(applications);

        Option transformers = new Option(
                null, "transformers", true, "Path to transformers directory.");
        transformers.setRequired(false);
        options.addOption(transformers);

        Option dataset = new Option(
                null, "dataset", true, "Path to dataset directory.");
        dataset.setRequired(false);
        options.addOption(dataset);

        Option output = new Option(
                "o", "output", true, "Path to output directory");
        output.setRequired(true);
        options.addOption(output);

        Option filter = new Option(
                null, "filter", true, "Filter strategy. Values: "
                + "'no-filter', 'isomorphic', 'diff'");
        filter.setRequired(false);
        options.addOption(filter);

        Option limit = new Option(
                null, "limit", true, "Iteration limit.");
        limit.setRequired(false);
        options.addOption(limit);

        Option ignoreDiscoveryIssues = new Option(
                null, "IHaveBadDiscoveryDefinition", false,
                "Ignore issues in discovery definition.");
        ignoreDiscoveryIssues.setRequired(false);
        options.addOption(ignoreDiscoveryIssues);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
            // IDEA fail to detect above as application end.
            throw new RuntimeException();
        }
    }

    static MeterRegistry createMeterRegistry() {
        return new SimpleMeterRegistry();
    }

    static List<DiscoveryStatisticsInPath> runDiscovery(
            DiscoveryBuilder builder, int limit,
            MeterRegistry registry, File outputDirectory) throws Exception {
        //
        List<DiscoveryStatisticsInPath> statistics = new ArrayList<>();
        Map<String, String> discoveryNames = new HashMap<>();
        builder.setStoreFactory((iri) -> {
            String name = "discovery_"
                    + String.format("%03d", discoveryNames.size());
            discoveryNames.put(iri, name);
            //return SampleStore.memoryStore();
            //return SampleStore.fileSystemStore(
            //        new File(outputDirectory, name + "/working/sample-store"),
            //        registry);
            return SampleStore.memoryMapStore(registry);
        });
        for (Discovery discovery : builder.create(registry)) {
            Node root = discovery.explore(limit);
            DiscoveryStatistics stats = discovery.getStatistics();
            String name = discoveryNames.get(discovery.getIri());
            File output = new File(outputDirectory, name);
            export(discovery, root, output);
            statistics.add(new DiscoveryStatisticsInPath(stats, name));
            discovery.cleanUp();
            // Print statistics during the execution.
            AppEntry.logMeterRegistry(registry);
            // TODO Optically delete the working directory here.
        }
        SummaryExport.export(statistics, outputDirectory);
        return statistics;
    }

    private static void export(Discovery discovery, Node root, File output)
            throws IOException {
        LOG.debug("Exporting ...");
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
                discovery.getSampleStore(),
                new File(output, "node-data-samples"));
    }

    static void logMeterRegistry(MeterRegistry registry) {
        String message = "Runtime statistics:" + System.lineSeparator()
                + "    total time        :  %8d s" + System.lineSeparator()
                + "    file system IO    :  %8d s" + System.lineSeparator()
                + "    repository create :  %8d s" + System.lineSeparator()
                + "    repository update :  %8d s" + System.lineSeparator()
                + "    repository ask    :  %8d s" + System.lineSeparator()
                + "    filter.isomorphic :  %8d s" + System.lineSeparator()
                + "    filter.diff.create:  %8d s" + System.lineSeparator()
                + "    filter.diff.match :  %8d s" + System.lineSeparator()
                + "    store.map         :  %8d s" + System.lineSeparator();
        LOG.info(String.format(message,
                (int) registry.timer(MeterNames.DISCOVERY_TIME)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.DATA_SAMPLE_STORAGE)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.CREATE_REPOSITORY)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.UPDATE_DATA)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.MATCH_DATA)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.FILTER_ISOMORPHIC)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.FILTER_DIFF_CREATE)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.FILTER_DIFF_FILTER)
                        .totalTime(TimeUnit.SECONDS),
                (int) registry.timer(MeterNames.STORE_MAP_MEMORY)
                        .totalTime(TimeUnit.SECONDS)));
    }

}
