package com.linkedpipes.discovery.cli.pipeline;

import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.rdf.LangString;

public class DatasetReference {

    public final String iri;

    public final LangString title;

    public final String sparqlEndpoint;

    public DatasetReference(Dataset dataset) {
        this.iri = dataset.iri;
        this.title = dataset.title;
        sparqlEndpoint = dataset.getSparqlEndpointOr(null);
    }

}
