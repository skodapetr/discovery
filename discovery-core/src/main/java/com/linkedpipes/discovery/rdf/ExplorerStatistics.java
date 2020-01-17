package com.linkedpipes.discovery.rdf;

import com.linkedpipes.discovery.SuppressFBWarnings;

import java.util.HashSet;
import java.util.Set;

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

    /**
     * Number of pipelines, i.e. count of all applications on all nodes.
     */
    public int pipelines;

    /**
     * Matching application in the whole tree.
     */
    public Set<String> applications = new HashSet<>();

}
