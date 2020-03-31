package com.linkedpipes.discovery.cli.factory;

import java.io.File;

public class BuilderConfiguration {

    public static final int DEFAULT_MAX_NODE_EXPANSION = 5;

    public Integer levelLimit;

    public File output;

    public String filter;

    public Boolean ignoreIssues;

    public Boolean useDataSampleMapping;

    public String store;

    public Integer maxNodeExpansionTimeSeconds = DEFAULT_MAX_NODE_EXPANSION;

    public Boolean resume;

    public Integer discoveryTimeLimit;

    public Boolean useStrongGroups;

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

    public BuilderConfiguration merge(BuilderConfiguration configuration) {
        if (levelLimit == null) {
            levelLimit = configuration.levelLimit;
        }
        if (output == null) {
            output = configuration.output;
        }
        if (filter == null) {
            filter = configuration.filter;
        }
        if (ignoreIssues == null) {
            ignoreIssues = configuration.ignoreIssues;
        }
        if (useDataSampleMapping == null) {
            useDataSampleMapping = configuration.useDataSampleMapping;
        }
        if (store == null) {
            store = configuration.store;
        }
        if (resume == null) {
            resume = configuration.resume;
        }
        if (discoveryTimeLimit == null) {
            discoveryTimeLimit = configuration.discoveryTimeLimit;
        }
        if (useStrongGroups == null) {
            useStrongGroups = configuration.useStrongGroups;
        }
        if (urlCache == null) {
            urlCache = configuration.urlCache;
        }
        return this;
    }

}
