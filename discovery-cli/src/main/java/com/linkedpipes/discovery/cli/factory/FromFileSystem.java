package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import com.linkedpipes.discovery.sample.SampleStore;
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
    public List<Discovery> create(MeterRegistry registry) throws Exception {
        String name = directory.getName();
        String datasetIri = "http://localhost/" + name;
        File datasetSampleFile = new File(directory, "sample.ttl");
        Dataset dataset = new Dataset(
                datasetIri, RdfAdapter.asStatements(datasetSampleFile));
        String iri = directory.toURI().toString();
        SampleStore sampleStore = storeFactory.apply(iri);
        NodeFilter filterStrategy = createFilterStrategy(
                sampleStore, registry);
        Discovery discovery = new Discovery(
                iri, dataset, transformers, applications,
                filterStrategy, sampleStore, registry);
        return Collections.singletonList(discovery);
    }

}
