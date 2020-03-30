package com.linkedpipes.discovery.cli.model;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.rdf.LangString;
import com.linkedpipes.discovery.statistics.DiscoveryStatistics;
import com.linkedpipes.discovery.SuppressFBWarnings;

import java.util.List;

/**
 * From core module, we got statistics. But in order to generate summary
 * we also need to remember some identification (path) for the statistics.
 */
@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class NamedDiscoveryStatistics extends DiscoveryStatistics {

    public static class DatasetReference {

        public final String iri;

        public final LangString title;

        public final String sparqlEndpoint;

        public DatasetReference(
                String iri, LangString label, String sparqlEndpoint) {
            this.iri = iri;
            this.title = label;
            this.sparqlEndpoint = sparqlEndpoint;
        }
    }

    public String name;

    public final List<Application> applications;

    public final List<Transformer> transformers;

    public final DatasetReference dataset;

    public NamedDiscoveryStatistics(
            String name, DiscoveryStatistics source,
            Discovery discovery, Dataset dataset) {
        this.levels = source.levels;
        this.discoveryIri = source.discoveryIri;
        this.name = name;
        this.applications = discovery.getApplications();
        this.transformers = discovery.getTransformers();
        this.dataset = new DatasetReference(
                dataset.iri, dataset.title,
                dataset.configuration.service.endpoint);
    }

}
