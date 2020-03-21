package com.linkedpipes.discovery.listeners;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryListener;
import com.linkedpipes.discovery.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

public class LimitByTime implements DiscoveryListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(LimitByTime.class);

    private Instant start;

    private long timeLimitInMinutes;

    public LimitByTime(long timeLimitInMinutes) {
        this.timeLimitInMinutes = timeLimitInMinutes;
    }

    @Override
    public boolean discoveryWillRun(Discovery context) {
        start = Instant.now();
        return true;
    }

    @Override
    public boolean nodeDidExpand(Node node) {
        long durationInMinutes =
                Duration.between(start, Instant.now()).toMinutes();
        if (durationInMinutes < timeLimitInMinutes) {
            return true;
        } else {
            LOG.info("Discovery time limit reached {} minutes (limit {})",
                    durationInMinutes, timeLimitInMinutes);
            return false;
        }
    }

}
