package com.linkedpipes.discovery;

/**
 * Names of meters used in the application.
 */
public final class MeterNames {

    /**
     * Time of running whole discovery process for a node.
     */
    public static final String DISCOVERY_TIME = "discovery";

    /**
     * Time used to load/save data samples in/to file system.
     */
    public static final String DATA_SAMPLE_STORAGE = "filesystem";

    /**
     * Time used to create RDF4J repository for a data samples, include
     * loading the data.
     */
    public static final String CREATE_REPOSITORY = "repository.create";

    /**
     * Time consumed by executing SPARQL UPDATE, i.e. application of
     * transformers.
     */
    public static final String UPDATE_DATA = "data.update";

    /**
     * Time consumed by executing SPARQL ASK query, i.e. matching
     * applications and transformers.
     */
    public static final String MATCH_DATA = "data.ask";

    /**
     * TIme used in isomorphic-based filter to compare nodes.
     */
    public static final String FILTER_ISOMORPHIC =
            "filter.isomorphic.compare";

    /**
     * Time used in diff-based filters used to crate diffs.
     */
    public static final String FILTER_DIFF_CREATE = "filter.diff.create";

    /**
     * Time used to compare Node data samples in diff-based filter.
     */
    public static final String FILTER_DIFF_FILTER = "filter.diff.compare";

}
