package com.linkedpipes.discovery.cli.model;

import com.linkedpipes.discovery.DiscoveryStatistics;
import com.linkedpipes.discovery.SuppressFBWarnings;

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
