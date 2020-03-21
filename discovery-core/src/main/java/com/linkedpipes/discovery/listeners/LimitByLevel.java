package com.linkedpipes.discovery.listeners;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryListener;
import com.linkedpipes.discovery.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stop iteration on given level.
 */
public class LimitByLevel implements DiscoveryListener {

    private static final Logger LOG = LoggerFactory.getLogger(LimitByLevel.class);

    private final int levelLimit;

    public LimitByLevel(int levelLimit) {
        this.levelLimit = levelLimit;
    }

    @Override
    public boolean discoveryWillRun(Discovery context) {
        Node node = context.getQueue().peek();
        if (node == null) {
            return true;
        }
        if (node.getLevel() < levelLimit) {
            return true;
        } else {
            LOG.info("Level limit reached: {} limit: {}",
                    node.getLevel(), levelLimit);
            return false;
        }
    }

    @Override
    public boolean levelDidEnd(int level) {
        return level < levelLimit;
    }

}
