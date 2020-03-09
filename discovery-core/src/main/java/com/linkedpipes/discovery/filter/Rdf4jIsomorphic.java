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

public class Rdf4jIsomorphic implements NodeFilter {

    private static final Logger LOG =
            LoggerFactory.getLogger(Rdf4jIsomorphic.class);

    private List<SampleRef> samples = null;

    private final SampleStore sampleStore;

    private final Timer timer;

    public Rdf4jIsomorphic(SampleStore sampleStore, MeterRegistry registry) {
        this.sampleStore = sampleStore;
        this.timer = registry.timer(MeterNames.RDF4J_MODEL_ISOMORPHIC);
    }

    @Override
    public void init(Node root) {
        samples = new ArrayList<>();
    }

    @Override
    public void addNode(Node node) {
        sampleStore.addReferenceUser(node.getDataSampleRef());
        samples.add(node.getDataSampleRef());
    }

    @Override
    public boolean isNewNode(Node node, List<Statement> dataSample)
            throws DiscoveryException {
        for (SampleRef visitedRef : samples) {
            List<Statement> visitedSample = sampleStore.load(visitedRef);
            Instant start = Instant.now();
            boolean isIsomorphic = Models.isomorphic(dataSample, visitedSample);
            timer.record(Duration.between(start, Instant.now()));
            if (isIsomorphic) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void logAfterLevelFinished() {
        LOG.info("Number of data samples: {}", samples.size());
    }

}
