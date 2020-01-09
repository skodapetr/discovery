package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.rdf.LangString;

import java.util.ArrayList;
import java.util.List;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class Application {

    public static final String TYPE =
            "https://discovery.linkedpipes.com/vocabulary/"
                    + "ApplicationTemplate";

    public String iri;

    public LangString title = new LangString();

    public LangString description = new LangString();

    public String executor;

    public Configuration configurationTemplate;

    public List<PortTemplate> inputs = new ArrayList<>();

    public List<Feature> features = new ArrayList<>();

}
