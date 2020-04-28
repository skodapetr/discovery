package com.linkedpipes.discovery.cli.factory;

import java.io.File;

public class BuilderConfiguration {

    public static final int DEFAULT_MAX_NODE_EXPANSION = 5;

    public Integer levelLimit;

    public String output;

    public String filter;

    /**
     * We need this option for reading definition so it can not be null.
     */
    public boolean ignoreIssues = false;

    public boolean useDataSampleMapping = false;

    public String store;

    public Integer maxNodeExpansionTimeSeconds = DEFAULT_MAX_NODE_EXPANSION;

    public boolean resume = false;

    public Integer discoveryTimeLimit;

    public boolean useStrongGroups = false;

    public File urlCache;

    public File reportFile() {
        if (!ignoreIssues) {
            return null;
        }
        return new File(output, "builder-report.txt");
    }

    public BuilderConfiguration copy() {
        BuilderConfiguration result = new BuilderConfiguration();
        result.levelLimit = levelLimit;
        result.output = output;
        result.filter = filter;
        result.ignoreIssues = ignoreIssues;
        result.useDataSampleMapping = useDataSampleMapping;
        result.store = store;
        result.resume = resume;
        result.discoveryTimeLimit = discoveryTimeLimit;
        return result;
    }

    public BuilderConfiguration mergeWithRuntime(
            BuilderConfiguration configuration) {
        BuilderConfiguration result = this.copy();
        if (result.levelLimit == null) {
            result.levelLimit = configuration.levelLimit;
        }
        if (result.output == null) {
            result.output = configuration.output;
        }
        if (result.filter == null) {
            result.filter = configuration.filter;
        }
        result.ignoreIssues |= configuration.ignoreIssues;
        result.useDataSampleMapping |= configuration.useDataSampleMapping;
        if (result.store == null) {
            result.store = configuration.store;
        }
        result.resume |= configuration.resume;
        if (result.discoveryTimeLimit == null) {
            result.discoveryTimeLimit = configuration.discoveryTimeLimit;
        }
        result.useStrongGroups |= configuration.useStrongGroups;
        if (result.urlCache == null) {
            result.urlCache = configuration.urlCache;
        }
        return result;
    }

    public static BuilderConfiguration defaultConfiguration() {
        BuilderConfiguration result = new BuilderConfiguration();
        result.output = "./output";
        result.filter = "diff";
        result.store = "memory";
        return result;
    }

}
