package com.linkedpipes.discovery.rdf;

import com.linkedpipes.discovery.SuppressFBWarnings;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class ExplorerStatistics {

    /**
     * Total number of generated nodes, created by expansion.
     */
    public int generated;

    /**
     * Output tree size.
     */
    public int finalSize;

    /**
     * Number of nodes filtered out.
     */
    public int filteredOut;

}
