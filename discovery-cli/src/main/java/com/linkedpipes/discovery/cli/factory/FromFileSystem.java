package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.filter.FilterStrategy;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.node.NodeFacade;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import io.micrometer.core.instrument.MeterRegistry;

import java.io.File;
import java.util.Collections;
import java.util.List;

public class FromFileSystem extends DiscoveryBuilder {

    protected File directory;

    public FromFileSystem(File directory) {
        this.directory = directory;
    }

    @Override
    public List<Discovery> create(
            MeterRegistry registry, DirectorySource directorySource)
            throws Exception {
        String name = directory.getName();
        String datasetIri = "http://localhost/" + name;
        File datasetSampleFile = new File(directory, "sample.ttl");
        Dataset dataset = new Dataset(
                datasetIri, RdfAdapter.asStatements(datasetSampleFile));
        String iri = directory.toURI().toString();
        NodeFacade nodeFacade = NodeFacade.withFileSystemStorage(
                directorySource.get(iri), registry);
        FilterStrategy filterStrategy = getFilterStrategy(
                nodeFacade, registry);
        Discovery discovery = new Discovery(
                iri, dataset, transformers, applications,
                filterStrategy, nodeFacade, registry);
        return Collections.singletonList(discovery);
    }

}
