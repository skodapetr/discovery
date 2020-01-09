package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;

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

    private Timer timer;

    public Rdf4jIsomorphic(MeterRegistry registry) {
        this.timer = registry.timer(MeterNames.FILTER_ISOMORPHIC);
    }

    @Override
    public void init(Node root) {
        samples = new ArrayList<>();
    }

    @Override
    public void addNode(Node node) {
        samples.add(node.getDataSample());
    }

    @Override
    public boolean isNewNode(Node node) {
        return timer.record(() -> {
            List<Statement> nodeSample = node.getDataSample();
            for (List<Statement> visitedSample : samples) {
                if (Models.isomorphic(nodeSample, visitedSample)) {
                    return false;
                }
            }
            return true;
        });
    }

}
