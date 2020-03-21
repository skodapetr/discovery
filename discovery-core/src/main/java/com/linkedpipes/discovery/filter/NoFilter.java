package com.linkedpipes.discovery.filter;

import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.util.List;

/**
 * All is new, i.e. no filter.
 */
public class NoFilter implements NodeFilter {

    @Override
    public boolean isNewNode(Node node, List<Statement> dataSample) {
        return true;
    }

    @Override
    public void save(File directory, SampleRefToString sampleRefToString)  {
        // No action.
    }

    @Override
    public void load(
            File directory, StringToSampleRef stringToSampleRef)  {
        // No action.
    }
    
}
