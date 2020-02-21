package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.rdf.LangString;
import org.eclipse.rdf4j.model.Statement;

import java.util.ArrayList;
import java.util.List;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class Dataset {

    public static final String TYPE =
            "https://discovery.linkedpipes.com/vocabulary/"
                    + "DataSourceTemplate";

    public String iri;

    public LangString title = new LangString();

    public List<Statement> sample = new ArrayList<>();

    public Dataset() {
    }

    public Dataset(String iri, List<Statement> sample) {
        this.iri = iri;
        this.sample = sample;
    }

}
