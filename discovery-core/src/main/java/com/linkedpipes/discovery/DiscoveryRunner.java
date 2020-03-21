package com.linkedpipes.discovery;

import com.linkedpipes.discovery.node.AskNode;
import com.linkedpipes.discovery.node.ExpandNode;
import com.linkedpipes.discovery.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;

/**
 * Expand tree inside the a {@link Discovery}.
 */
public class DiscoveryRunner {

    private static final Logger LOG =
            LoggerFactory.getLogger(DiscoveryRunner.class);

    /**
     * Run discovery by exploring given context.
     */
    public void explore(Discovery context)
            throws DiscoveryException {
        LOG.info("Running discovery {}", context.getIri());
        ExpandNode expander = createExpander(context);
        if (!onDiscoveryWillRun(context)) {
            LOG.info("Aborting discovery.");
            onDiscoveryDidRun(context);
            return;
        }
        final Deque<Node> queue = context.getQueue();
        if (queue.isEmpty()) {
            LOG.info("Nothing to expand.");
            return;
        }
        int lastLevel = queue.peek().getLevel();
        while (!queue.isEmpty()) {
            boolean shouldContinue = expandLevel(expander, context);
            if (!shouldContinue) {
                break;
            }
            LOG.info("Level {} expanded", lastLevel);
            if (!onLevelDidEnd(context, lastLevel)) {
                break;
            }
            lastLevel++;
        }
        onDiscoveryDidRun(context);
        LOG.info("Discovery finished.");
    }

    private ExpandNode createExpander(Discovery context) {
        AskNode askNode = new AskNode(
                context.getApplications(),
                context.getTransformers(),
                context.getRegistry());
        return new ExpandNode(
                context.getStore(), context.getFilter(), askNode,
                context.getDataSampleTransformer(), context.getRegistry());
    }

    private boolean onDiscoveryWillRun(Discovery context) {
        boolean result = true;
        for (DiscoveryListener listener : context.getListeners()) {
            result &= listener.discoveryWillRun(context);
        }
        return result;
    }

    public boolean expandLevel(ExpandNode expander, Discovery context)
            throws DiscoveryException {
        final Deque<Node> queue = context.getQueue();
        if (queue.isEmpty()) {
            return false;
        }
        int lastLevel = queue.peek().getLevel();
        while (!queue.isEmpty()) {
            if (queue.peek().getLevel() > lastLevel) {
                return true;
            }
            Node node = queue.pop();
            if (!onNodeWillExpand(context, node)) {
                return false;
            }
            try {
                if (node.isRedundant()) {
                    // We do not need to expand redundant nodes,
                    // the redundant flag may got assigned by a listener.
                    node.setExpanded(true);
                } else {
                    expander.expand(node);
                }
            } catch (OutOfMemoryError ex) {
                LOG.info("Out of memory!");
                return false;
            }
            queue.addAll(node.getNext());
            if (!onNodeDidExpand(context, node)) {
                return false;
            }
        }
        return true;
    }

    private boolean onLevelDidEnd(Discovery context, int nextLevel) {
        boolean result = true;
        for (DiscoveryListener listener : context.getListeners()) {
            result &= listener.levelDidEnd(nextLevel);
        }
        return result;
    }

    private boolean onNodeWillExpand(Discovery context, Node node) {
        boolean result = true;
        for (DiscoveryListener listener : context.getListeners()) {
            result &= listener.nodeWillExpand(node);
        }
        return result;
    }

    private boolean onNodeDidExpand(Discovery context, Node node) {
        boolean result = true;
        for (DiscoveryListener listener : context.getListeners()) {
            result &= listener.nodeDidExpand(node);
        }
        return result;
    }

    private void onDiscoveryDidRun(Discovery context) {
        for (DiscoveryListener listener : context.getListeners()) {
            listener.discoveryDidRun();
        }
    }

}
