package com.linkedpipes.discovery.rdf;

import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Allow representation of string in multiple languages.
 */
public class LangString {

    private static final Logger LOG = LoggerFactory.getLogger(LangString.class);

    private final Map<String, String> values = new HashMap<>();

    public void add(String language, String value) {
        values.put(language, value);
    }

    public void add(Value value) {
        if (value instanceof Literal) {
            Literal literal = (Literal) value;
            String language = literal.getLanguage().orElse("");
            add(language, literal.stringValue());
        }
        LOG.debug("Ignored non-literal value: {}", value);
    }

    public Map<String, String> getValues() {
        return Collections.unmodifiableMap(values);
    }

    public static LangString instance(String value) {
        LangString result = new LangString();
        result.add("", value);
        return result;
    }

    public static LangString instance(String language, String value) {
        LangString result = new LangString();
        result.add(language, value);
        return result;
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof LangString)) {
            return false;
        }
        return values.equals(((LangString) obj).values);
    }

    public String asString() {
        if (values.isEmpty()) {
            return "";
        }
        if (values.containsKey("cs")) {
            return values.get("cs");
        }
        if (values.containsKey("en")) {
            return values.get("en");
        }
        return values.values().iterator().next();
    }

}
