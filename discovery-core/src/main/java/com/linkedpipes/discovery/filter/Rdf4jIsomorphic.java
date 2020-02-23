package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.node.NodeFacade;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Times for https---nkod.opendata.cz-sparql 02:15
 * Exploration statistics:
 * - generated         : 1024
 * - output tree size  : 256
 * Runtime statistics:
 * - filter.isomorphic.compare total: 126 s
 *
 * <p>Times for http---data.open.ac.uk-query
 */
public class Rdf4jIsomorphic implements FilterStrategy {

    /**
     * Store data sample by size.
     */
    private List<List<Statement>> samples = null;

    private final NodeFacade nodeFacade;

    private final Timer timer;

    public Rdf4jIsomorphic(NodeFacade nodeFacade, MeterRegistry registry) {
        this.nodeFacade = nodeFacade;
        this.timer = registry.timer(MeterNames.FILTER_ISOMORPHIC);
    }

    @Override
    public void init(Node root) {
        samples = new ArrayList<>();
    }

    @Override
    public void addNode(Node node) throws DiscoveryException {
        samples.add(nodeFacade.getDataSample(node));
    }

    @Override
    public boolean isNewNode(Node node) throws DiscoveryException {
        Instant start = Instant.now();
        try {
            List<Statement> nodeSample = nodeFacade.getDataSample(node);
            for (List<Statement> visitedSample : samples) {
                if (Models.isomorphic(nodeSample, visitedSample)) {
                    return false;
                }
            }
            return true;
        } finally {
            timer.record(Duration.between(start, Instant.now()));
        }
    }

}
