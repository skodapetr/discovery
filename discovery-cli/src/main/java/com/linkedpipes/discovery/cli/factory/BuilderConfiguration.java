package com.linkedpipes.discovery.cli.factory;

import java.io.File;

public class BuilderConfiguration {

    public static final String DEFAULT_FILTER = "diff";

    public static final String DEFAULT_STORE = "memory-disk";

    public static final int DEFAULT_MAX_NODE_EXPANSION = 5;

    public int limit;

    public File output;

    public String filter = DEFAULT_FILTER;

    public int threads;

    public boolean ignoreIssues = false;

    public boolean useDataSampleMapping = false;

    public String store = DEFAULT_STORE;

    public int maxNodeExpansionTimeSeconds = DEFAULT_MAX_NODE_EXPANSION;

    public File reportFile() {
        if (!ignoreIssues) {
            return null;
        }
        return new File(output, "builder-report.txt");
    }

    public BuilderConfiguration copy() {
        BuilderConfiguration result = new BuilderConfiguration();
        result.limit = limit;
        result.output = output;
        result.filter = filter;
        result.threads = threads;
        result.ignoreIssues = ignoreIssues;
        result.useDataSampleMapping = useDataSampleMapping;
        result.store = store;
        return result;
    }

}
