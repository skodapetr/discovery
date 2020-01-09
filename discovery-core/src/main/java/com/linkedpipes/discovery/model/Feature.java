package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.rdf.LangString;

import java.util.ArrayList;
import java.util.List;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class Feature {

    public LangString title = new LangString();

    public List<Descriptor> descriptors = new ArrayList<>();

}
