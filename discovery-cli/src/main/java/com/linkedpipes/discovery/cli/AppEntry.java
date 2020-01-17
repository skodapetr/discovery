package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.cli.export.DataSamplesExport;
import com.linkedpipes.discovery.cli.export.NodeToName;
import com.linkedpipes.discovery.cli.export.JsonExport;
import com.linkedpipes.discovery.cli.export.SummaryExport;
import com.linkedpipes.discovery.cli.factory.DiscoveryBuilder;
import com.linkedpipes.discovery.cli.factory.FromDiscoveryUrl;
import com.linkedpipes.discovery.cli.factory.FromFileSystem;
import com.linkedpipes.discovery.cli.export.GephiExport;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.rdf.ExplorerStatistics;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
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
import java.util.HashMap;
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
        runDiscovery(builder, limit, output);
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

    static Map<String, ExplorerStatistics> runDiscovery(
            DiscoveryBuilder builder, int limit, File outputRoot)
            throws Exception {
        SimpleMeterRegistry memoryRegistry = new SimpleMeterRegistry();
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        registry.add(memoryRegistry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        Map<String, ExplorerStatistics> statistics = new HashMap<>();
        for (Discovery discovery : builder.create(registry)) {
            LOG.info("Running exploration for: {}", discovery.getDataset().iri);
            Node root = discovery.explore(limit);
            ExplorerStatistics stats = discovery.getStatistics();
            LOG.info("Exploration statistics:"
                            + "\n    generated         : {}"
                            + "\n    output tree size  : {}",
                    stats.generated, stats.finalSize);
            String outputName =
                    "discovery_" + String.format("%03d", statistics.size());
            File output = new File(outputRoot, outputName);
            export(discovery, root, output);
            statistics.put(outputName, stats);
        }
        SummaryExport.export(statistics, new File(outputRoot, "summary.csv"));
        logMeterRegistry(registry);
        LOG.info("All done.");
        return statistics;
    }

    private static void export(Discovery discovery, Node root, File output)
            throws IOException {
        LOG.info("Exporting ...");
        NodeToName nodeToName = new NodeToName(root);
        GephiExport.export(root,
                new File(output, "gephi-edges.csv"),
                new File(output, "gephi-vertices.csv"),
                nodeToName, discovery.getApplications());
        JsonExport.export(
                discovery, root, new File(output, "pipelines.json"),
                nodeToName);
        DataSamplesExport.export(
                root, nodeToName, new File(output, "data-samples"));
    }

    private static void logMeterRegistry(MeterRegistry registry) {
        LOG.info("Runtime statistics:");
        logTimeSummary(registry.timer(MeterNames.CREATE_REPOSITORY));
        logTimeSummary(registry.timer(MeterNames.UPDATE_DATA));
        logTimeSummary(registry.timer(MeterNames.MATCH_DATA));
        logTimeSummary(registry.timer(MeterNames.FILTER_ISOMORPHIC));
        logTimeSummary(registry.timer(MeterNames.FILTER_DIFF_CREATE));
        logTimeSummary(registry.timer(MeterNames.FILTER_DIFF_FILTER));
    }

    private static void logTimeSummary(Timer timer) {
        LOG.info("  {} total: {} s",
                timer.getId().getName(),
                (int) timer.totalTime(TimeUnit.SECONDS));
    }

}
