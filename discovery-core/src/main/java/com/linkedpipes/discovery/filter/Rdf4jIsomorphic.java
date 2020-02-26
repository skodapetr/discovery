package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.SampleRef;
import com.linkedpipes.discovery.sample.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Rdf4jIsomorphic implements NodeFilter {

    private static final Logger LOG =
            LoggerFactory.getLogger(Rdf4jIsomorphic.class);

    private List<SampleRef> samples = null;

    private final SampleStore sampleStore;

    private final Timer timer;

    public Rdf4jIsomorphic(SampleStore sampleStore, MeterRegistry registry) {
        this.sampleStore = sampleStore;
        this.timer = registry.timer(MeterNames.FILTER_ISOMORPHIC);
    }

    @Override
    public void init(Node root) {
        samples = new ArrayList<>();
    }

    @Override
    public void addNode(Node node) {
        sampleStore.addReferenceUser(node.getDataSampleRef());
    }

    @Override
    public boolean isNewNode(Node node) throws DiscoveryException {
        Instant start = Instant.now();
        try {
            List<Statement> nodeSample =
                    sampleStore.load(node.getDataSampleRef());
            for (SampleRef visitedRef : samples) {
                List<Statement> visitedSample = sampleStore.load(visitedRef);
                if (Models.isomorphic(nodeSample, visitedSample)) {
                    return false;
                }
            }
            return true;
        } finally {
            timer.record(Duration.between(start, Instant.now()));
        }
    }

    @Override
    public void logAfterLevelFinished() {
        LOG.info("Number of data samples: {}", samples.size());
    }

}
