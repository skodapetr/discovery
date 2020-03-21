package com.linkedpipes.discovery.listeners;

import com.linkedpipes.discovery.DiscoveryListener;
import com.linkedpipes.discovery.node.Node;

import java.time.Duration;
import java.time.Instant;

public class LimitByNodeExpansionTime implements DiscoveryListener {

    private Instant nodeExpansionStart;

    private final long maxNodeExpansionTimeMs;

    public LimitByNodeExpansionTime(long maxNodeExpansionTimeMs) {
        this.maxNodeExpansionTimeMs = maxNodeExpansionTimeMs;
    }

    @Override
    public boolean nodeWillExpand(Node node) {
        nodeExpansionStart = Instant.now();
        return true;
    }

    @Override
    public boolean nodeDidExpand(Node node) {
        long duration = Duration.between(
                nodeExpansionStart, Instant.now()).toMillis();
        return duration < maxNodeExpansionTimeMs;
    }

}
