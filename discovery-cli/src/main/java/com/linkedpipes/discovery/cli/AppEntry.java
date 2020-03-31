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

    public static void main(String[] args) throws Exception {
        (new AppEntry()).run(args);
    }

    @SuppressFBWarnings(value = {"DM_EXIT"})
    public void run(String[] args) throws Exception {
        CommandLine cmd = parseArgs(args);
        BuilderConfiguration configuration = loadConfiguration(cmd);
        if (cmd.hasOption("Experiment")) {
            // An experiment is handled by a different class.
            RunExperiment runner = new RunExperiment(configuration);
            runner.run(cmd.getOptionValue("Experiment"));
        } else if (cmd.hasOption("Discovery")) {
            RunDiscovery runner = new RunDiscovery(configuration);
            runner.run(
                    cmd.getOptionValue("Discovery"),
                    (discovery, dataset, statistics, name) -> {
                    });
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
                "e", "Experiment", true, "Url of an experiment to run.");
        experiment.setRequired(false);
        options.addOption(experiment);

        Option discovery = new Option(
                "d", "Discovery", true, "Url of a discovery to run.");
        discovery.setRequired(false);
        options.addOption(discovery);

        //

        Option output = new Option(
                "o", "Output", true, "Path to output directory");
        output.setRequired(false);
        options.addOption(output);

        Option filter = new Option(
                null, "Filter", true, "Filter strategy. Values: "
                + "'no-filter', 'isomorphic', 'diff'");
        filter.setRequired(false);
        options.addOption(filter);

        Option limit = new Option(
                null, "LevelLimit", true, "Iteration limit.");
        limit.setRequired(false);
        options.addOption(limit);

        Option strongGroups = new Option(
                null, "StrongGroups", false, "Use transformer groups.");
        strongGroups.setRequired(false);
        options.addOption(strongGroups);

        Option ignoreDiscoveryIssues = new Option(
                null, "IHaveBadDiscoveryDefinition", false,
                "Ignore issues in discovery definition.");
        ignoreDiscoveryIssues.setRequired(false);
        options.addOption(ignoreDiscoveryIssues);

        Option useMapping = new Option(
                null, "UseMapping", false,
                "Use statements mapping, can reduce memory "
                        + "usage in exchange for extra CPU load.");
        useMapping.setRequired(false);
        options.addOption(useMapping);

        Option store = new Option(
                null, "Store", true, "Store strategy. Values: "
                + "'memory', 'diff', 'disk', 'cache-memory-disk'");
        store.setRequired(false);
        options.addOption(store);

        Option resume = new Option(
                null, "Resume", false,
                "If used and output exists try to resume execution.");
        resume.setRequired(false);
        options.addOption(resume);

        Option discoveryLimit = new Option(
                null, "DiscoveryTimeLimit", true,
                "Time limit in minutes on a single discovery run.");
        discoveryLimit.setRequired(false);
        options.addOption(discoveryLimit);

        Option urlCache = new Option(
                null, "UrlCache", true, "Directory to use for URL cache.");
        store.setRequired(false);
        options.addOption(urlCache);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
            // Some IDEs fail to detect above as application end.
            throw new RuntimeException();
        }
    }

    private BuilderConfiguration loadConfiguration(CommandLine cmd) {
        BuilderConfiguration configuration = new BuilderConfiguration();
        if (cmd.hasOption("Output")) {
            configuration.output = new File(cmd.getOptionValue("Output"));
        }
        if (cmd.hasOption("Filter")) {
            configuration.filter = cmd.getOptionValue("Filter");
        }
        if (cmd.hasOption("LevelLimit")) {
            configuration.levelLimit =
                    Integer.parseInt(cmd.getOptionValue("LevelLimit"));
        }
        if (cmd.hasOption("StrongGroups")) {
            configuration.useStrongGroups = true;
        }
        if (cmd.hasOption("IHaveBadDiscoveryDefinition")) {
            configuration.ignoreIssues = true;
        }
        if (cmd.hasOption("UseMapping")) {
            configuration.useDataSampleMapping = true;
        }
        if (cmd.hasOption("Store")) {
            configuration.store = cmd.getOptionValue("Store");
        }
        if (cmd.hasOption("Resume")) {
            configuration.resume = true;
        }
        if (cmd.hasOption("DiscoveryTimeLimit")) {
            configuration.discoveryTimeLimit =
                    Integer.parseInt(cmd.getOptionValue("DiscoveryTimeLimit"));
        }
        if (cmd.hasOption("UrlCache")) {
            configuration.urlCache = new File(cmd.getOptionValue("UrlCache"));
        }
        return configuration;
    }

}
