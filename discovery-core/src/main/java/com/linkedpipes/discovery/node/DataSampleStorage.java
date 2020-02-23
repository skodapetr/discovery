package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.DiscoveryException;
import org.eclipse.rdf4j.model.Statement;

import java.util.List;

/**
 * Handles node data samples.
 */
public interface DataSampleStorage {

    void setDataSample(Node node, List<Statement> dataSample)
            throws DiscoveryException;

    List<Statement> getDataSample(Node node) throws DiscoveryException;

    void deleteDataSample(Node node);

    void cleanUp() throws DiscoveryException;

}

