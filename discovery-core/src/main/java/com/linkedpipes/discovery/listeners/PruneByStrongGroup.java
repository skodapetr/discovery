package com.linkedpipes.discovery.listeners;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryListener;
import com.linkedpipes.discovery.model.TransformerGroup;
import com.linkedpipes.discovery.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * For a node we determine it's group by last applied transformer.
 *
 * <p>If there are any nodes in next that are from the same group we
 * mark all other as redundant. For all from the same group we
 * preserve only one, with the smallest transformer IRI.
 */
public class PruneByStrongGroup implements DiscoveryListener {

    private static final Logger LOG =
            LoggerFactory.getLogger(PruneByStrongGroup.class);

    private List<TransformerGroup> groups;

    public PruneByStrongGroup(List<TransformerGroup> groups) {
        this.groups = groups;
    }

    @Override
    public boolean discoveryWillRun(Discovery context) {
        LOG.info("Using pruning with {} groups", groups.size());
        return true;
    }

    @Override
    public boolean nodeDidExpand(Node node) {
        Map<TransformerGroup, List<Node>> nodesByGroup = new HashMap<>();
        for (Node next : node.getNext()) {
            TransformerGroup group = getGroup(next);
            if (group == null) {
                continue;
            }
            if (!nodesByGroup.containsKey(group)) {
                nodesByGroup.put(group, new ArrayList<>());
            }
            nodesByGroup.get(group).add(next);
        }
        TransformerGroup nodeGroup = getGroup(node);
        if (nodeGroup != null && nodesByGroup.containsKey(nodeGroup)) {
            // Disable all other and preserve only one from the group.
            node.getNext().forEach(next -> next.setRedundant(true));
            processGroup(nodeGroup, nodesByGroup.get(nodeGroup));
        } else {
            // From each group preserve only one.
            for (var entry : nodesByGroup.entrySet()) {
                processGroup(entry.getKey(), entry.getValue());
            }
        }
        return true;
    }

    private TransformerGroup getGroup(Node node) {
        if (node.getTransformer() == null) {
            return null;
        }
        for (TransformerGroup group : groups) {
            if (group.transformers.contains(node.getTransformer().iri)) {
                return group;
            }
        }
        return null;
    }

    private void processGroup(TransformerGroup group, List<Node> nodes) {
        List<Integer> indices = nodes.stream()
                .map(node -> group.transformers.indexOf(
                        node.getTransformer().iri))
                .collect(Collectors.toList());
        Integer min = Collections.min(indices);
        for (int index = 0; index < nodes.size(); ++index) {
            Node node = nodes.get(index);
            if (indices.get(index).equals(min)) {
                node.setRedundant(false);
            } else {
                node.setRedundant(true);
            }
        }
    }

}
