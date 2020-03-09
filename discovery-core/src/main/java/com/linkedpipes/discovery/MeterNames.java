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
    public static final String FILE_STORE_IO = "store.file.io";

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
     * Time used to map statements in MapMemory store.
     */
    public static final String STORE_MAP_MEMORY = "store.map_memory";

    /**
     * Time used in diff-based filters used to crate diffs.
     */
    public static final String FILTER_DIFF_CREATE = "filter.diff.create";

    /**
     * Time used to compare Node data samples in diff-based filter,
     * i.e. execution of rdf4j Models.isomorphic.
     */
    public static final String RDF4J_MODEL_ISOMORPHIC =
            "rdf4j.isomorphic";

    /**
     * Time used to breakup statements in breakup store.
     */
    public static final String BREAKUP_STORE_ADD = "store.breakup.add";

    /**
     * Time used to construct statements in breakup store.
     */
    public static final String BREAKUP_STORE_GET = "store.breakup.get";

    /**
     * Time used to perform disk operation in breakup store.
     */
    public static final String BREAKUP_STORE_IO = "store.breakup.io";

    /**
     * Time to construct statements in diff store.
     */
    public static final String DIFF_STORE_CONSTRUCT = "store.diff.construct";

}
