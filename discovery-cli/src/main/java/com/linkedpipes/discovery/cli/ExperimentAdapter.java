package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.rdf.RdfAdapter;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class ExperimentAdapter {

    private static final Logger LOG =
            LoggerFactory.getLogger(ExperimentAdapter.class);

    private static final IRI HAS_DISCOVERY;

    private static final IRI HAS_CONFIGURATION;

    static {
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        HAS_DISCOVERY = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/experiment/"
                        + "hasDiscovery");
        HAS_CONFIGURATION = valueFactory.createIRI(
                "urn:DiscoveryConfiguration");
    }

    public static Experiment load(String url) throws IOException {
        Experiment result = new Experiment();
        List<Statement> statements;
        statements = RdfAdapter.asStatements(new URL(url));
        for (Statement statement : statements) {
            if (!(statement.getObject() instanceof Resource)) {
                continue;
            }
            Resource object = (Resource) statement.getObject();
            if (statement.getPredicate().equals(HAS_DISCOVERY)) {
                if (isListHead(statements, object)) {
                    result.discoveries.addAll(loadUrlList(statements, object));
                } else {
                    result.discoveries.add(object.stringValue());
                }
            }
            if (HAS_CONFIGURATION.equals(statement.getPredicate())) {
                if (statement.getObject() instanceof Resource) {
                    onConfiguration(object, statements, result);
                }
            }

        }
        return result;
    }

    private static boolean isListHead(
            List<Statement> statements, Resource resource) {
        for (Statement statement : statements) {
            if (!statement.getSubject().equals(resource)) {
                continue;
            }
            if (RDF.FIRST.equals(statement.getPredicate())) {
                return true;
            }
        }
        return false;
    }

    private static List<String> loadUrlList(
            List<Statement> statements, Resource resource) {
        List<String> result = new ArrayList<>();
        while (resource != null) {
            Resource rest = null;
            for (Statement statement : statements) {
                if (!statement.getSubject().equals(resource)) {
                    continue;
                }
                if (RDF.FIRST.equals(statement.getPredicate())) {
                    if (statement.getObject() instanceof Resource) {
                        result.add(statement.getObject().stringValue());
                    }
                }
                if (RDF.REST.equals(statement.getPredicate())) {
                    if (statement.getObject() instanceof Resource) {
                        rest = (Resource) statement.getObject();
                    }
                }
            }
            resource = rest;
        }
        return result;
    }

    private static void onConfiguration(
            Resource resource, List<Statement> statements,
            Experiment experiment) {
        LOG.info(
                "Loading experiment configuration from: {}",
                resource.stringValue());
        for (Statement statement : statements) {
            if (!statement.getSubject().equals(resource)) {
                continue;
            }
            //
            if (!(statement.getObject() instanceof Literal)) {
                continue;
            }
            Literal value = (Literal) statement.getObject();
            switch (statement.getPredicate().stringValue()) {
                case "urn:output":
                    experiment.output = value.stringValue();
                    break;
                default:
                    LOG.warn(
                            "Unknown configuration predicate: {}",
                            statement.getPredicate().stringValue());
                    break;
            }
        }
    }

}
