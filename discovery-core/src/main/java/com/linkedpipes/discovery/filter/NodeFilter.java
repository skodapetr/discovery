package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.DiscoveryListener;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.store.SampleRef;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface NodeFilter extends DiscoveryListener {

    @FunctionalInterface
    interface SampleRefToString {

        String convert(SampleRef ref);

    }

    @FunctionalInterface
    interface StringToSampleRef {

        SampleRef convert(String ref);

    }

    boolean isNewNode(Node node, List<Statement> dataSample)
            throws DiscoveryException;

    void save(
            File directory, SampleRefToString sampleRefToString)
            throws DiscoveryException, IOException;

    void load(
            File directory,
            StringToSampleRef stringToSampleRef)
            throws DiscoveryException, IOException;

}
