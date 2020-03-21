package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.List;

@SuppressFBWarnings(value = {"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class TransformerGroup {

    public static final String TYPE =
            "https://discovery.linkedpipes.com/vocabulary/discovery/"
                    + "TransformerGroup";

    public String iri;

    public List<String> transformers = new ArrayList<>();

}
