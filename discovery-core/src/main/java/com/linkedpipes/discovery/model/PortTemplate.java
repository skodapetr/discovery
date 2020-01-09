package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.rdf.LangString;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class PortTemplate {

    public String iri;

    public LangString title = new LangString();

}
