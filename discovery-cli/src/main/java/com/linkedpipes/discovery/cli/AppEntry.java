package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.cli.factory.BuilderConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;

/**
 * Application entry point.
 */
public class AppEntry {

    private static final String DEFAULT_FILTER_STRATEGY = "diff";

    public static void main(String[] args) throws Exception {
        (new AppEntry()).run(args);
    }

    @SuppressFBWarnings(value = {"DM_EXIT"})
    public void run(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        BuilderConfiguration configuration = loadConfiguration(cmd);
        if (cmd.hasOption("experiment")) {
            // An experiment is handled by a different class.
            ExperimentRunner runner = new ExperimentRunner(configuration);
            runner.run(cmd.getOptionValue("experiment"));
        } else if (cmd.hasOption("discovery")) {
            DiscoveryRunner runner = new DiscoveryRunner(configuration);
            runner.run(cmd.getOptionValue("discovery"));
        } else {
            System.out.println(
                    "Either 'discovery' or 'experiment' must be set.");
            System.exit(1);
        }
    }

    @SuppressFBWarnings(value = {"DM_EXIT"})
    private CommandLine parseArgs(String[] args) {
        Options options = new Options();

        Option experiment = new Option(
                "e", "experiment", true, "Url of an experiment to run.");
        experiment.setRequired(false);
        options.addOption(experiment);

        Option discovery = new Option(
                "d", "discovery", true, "Url of a discovery to run.");
        discovery.setRequired(false);
        options.addOption(discovery);

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

        Option threads = new Option(
                null, "threads", true, "Number of threads to use.");
        limit.setRequired(false);
        options.addOption(threads);

        Option ignoreDiscoveryIssues = new Option(
                null, "IHaveBadDiscoveryDefinition", false,
                "Ignore issues in discovery definition.");
        ignoreDiscoveryIssues.setRequired(false);
        options.addOption(ignoreDiscoveryIssues);

        Option useMapping = new Option(
                null, "UseMapping", false,
                "Use statements mapping, can reduce memory "
                        + "usage in exchange for extra CPU load..");
        useMapping.setRequired(false);
        options.addOption(useMapping);

        Option store = new Option(
                null, "store", true, "Store strategy. Values: "
                + "'memory', 'diff', 'disk', 'cache-memory-disk'");
        store.setRequired(false);
        options.addOption(store);

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

    private BuilderConfiguration loadConfiguration(CommandLine cmd) {
        BuilderConfiguration configuration = new BuilderConfiguration();
        configuration.limit =
                Integer.parseInt(cmd.getOptionValue("limit", "-1"));
        configuration.output = new File(cmd.getOptionValue("output"));
        configuration.filter =
                cmd.getOptionValue("filter", DEFAULT_FILTER_STRATEGY);
        configuration.threads =
                Integer.parseInt(cmd.getOptionValue("threads", "1"));
        configuration.store =
                cmd.getOptionValue("store", DEFAULT_FILTER_STRATEGY);
        if (cmd.hasOption("IHaveBadDiscoveryDefinition")) {
            configuration.ignoreIssues = true;
        }
        if (cmd.hasOption("UseMapping")) {
            configuration.useDataSampleMapping = true;
        }
        return configuration;
    }

}
