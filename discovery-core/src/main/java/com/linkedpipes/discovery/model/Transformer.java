package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.rdf.LangString;

import java.util.ArrayList;
import java.util.List;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class Transformer {

    public static final String TYPE =
            "https://discovery.linkedpipes.com/vocabulary/"
                    + "TransformerTemplate";

    public String iri;

    public LangString title = new LangString();

    public LangString description = new LangString();

    public Configuration configurationTemplate;

    public List<PortTemplate> inputs = new ArrayList<>();

    public List<PortTemplate> outputs = new ArrayList<>();

    public List<Feature> features = new ArrayList<>();


}
