package com.linkedpipes.discovery.cli.model;

import com.linkedpipes.discovery.DiscoveryStatistics;
import com.linkedpipes.discovery.SuppressFBWarnings;

/**
 * From core module, we got statistics. But in order to generate summary
 * we also need to remember some identification (path) for the statistics.
 */
@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class DiscoveryStatisticsInPath extends DiscoveryStatistics {
    
    public String path;
    
    public DiscoveryStatisticsInPath(DiscoveryStatistics source, String path) {
        this.levels = source.levels;
        this.discoveryIri = source.discoveryIri;
        this.dataset = source.dataset;
        this.path = path;
    }

}
