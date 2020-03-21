package com.linkedpipes.discovery.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.store.SampleGroup;
import com.linkedpipes.discovery.sample.store.SampleRef;
import com.linkedpipes.discovery.sample.store.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Rdf4jIsomorphic implements NodeFilter {

    private static final Logger LOG =
            LoggerFactory.getLogger(Rdf4jIsomorphic.class);

    private List<SampleRef> samples = null;

    private final SampleStore store;

    private final Timer timer;

    public Rdf4jIsomorphic(SampleStore sampleStore, MeterRegistry registry) {
        this.store = sampleStore;
        this.timer = registry.timer(MeterNames.RDF4J_MODEL_ISOMORPHIC);
    }

    @Override
    public boolean discoveryWillRun(Discovery context) {
        samples = new ArrayList<>();
        return true;
    }

    @Override
    public boolean nodeDidExpand(Node node) {
        try {
            List<Statement> dataSample = store.load(node.getDataSampleRef());
            samples.add(store.store(dataSample, SampleGroup.FILTER));
        } catch (DiscoveryException ex) {
            LOG.error("Can't store data sample.");
            return false;
        }
        return true;
    }

    @Override
    public boolean isNewNode(Node node, List<Statement> dataSample)
            throws DiscoveryException {
        for (SampleRef visitedRef : samples) {
            List<Statement> visitedSample = store.load(visitedRef);
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
    public boolean levelDidEnd(int level) {
        LOG.info("Number of data samples: {}", samples.size());
        return true;
    }

    @Override
    public void save(File directory, SampleRefToString sampleRefToString)
            throws IOException {
        List<String> data = samples.stream()
                .map(sampleRefToString::convert)
                .collect(Collectors.toList());
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(getDataFile(directory), data);
    }

    private File getDataFile(File directory) {
        return new File(directory, "rdf4j-isomorphic-filter.json");
    }

    @Override
    public void load(File directory, StringToSampleRef stringToSampleRef)
            throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String[] data = objectMapper
                .readValue(getDataFile(directory), String[].class);
        samples = new ArrayList<>(data.length);
        for (String item : data) {
            samples.add(stringToSampleRef.convert(item));
        }
    }

}
