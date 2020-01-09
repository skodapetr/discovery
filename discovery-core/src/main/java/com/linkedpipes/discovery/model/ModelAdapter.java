package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.rdf.RdfAdapter;
import com.linkedpipes.discovery.rdf.StatementUtils;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import com.linkedpipes.discovery.rdf.vocabulary.DCTERMS;
import com.linkedpipes.discovery.rdf.vocabulary.DISCOVERY;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ModelAdapter {

    public static final String APPLICATION =
            "https://discovery.linkedpipes.com/vocabulary/ApplicationTemplate";

    public static final String TRANSFORMER =
            "https://discovery.linkedpipes.com/vocabulary/TransformerTemplate";

    public static Application loadApplication(List<Statement> statements)
            throws UnexpectedInput {
        Resource resource =
                StatementUtils.findOneByClass(statements, APPLICATION);
        return loadApplication(statements, resource);
    }

    private static Application loadApplication(
            List<Statement> statements, Resource resource) {
        Application result = new Application();
        result.iri = resource.stringValue();
        for (Statement statement : statements) {
            if (!resource.equals(statement.getSubject())) {
                continue;
            }
            Value object = statement.getObject();
            switch (statement.getPredicate().stringValue()) {
                case DCTERMS.HAS_TITLE:
                    result.title.add(object);
                    break;
                case DCTERMS.HAS_DESCRIPTION:
                    result.description.add(object);
                    break;
                case DISCOVERY.HAS_CONFIGURATION_TEMPLATE:
                    if (object instanceof Resource) {
                        result.configurationTemplate =
                                loadConfiguration(
                                        statements, (Resource) object);
                    }
                    break;
                case DISCOVERY.HAS_EXECUTOR:
                    result.executor = object.stringValue();
                    break;
                case DISCOVERY.HAS_FEATURE:
                    if (object instanceof Resource) {
                        result.features.add(
                                loadFeature(statements, (Resource) object));
                    }
                    break;
                case DISCOVERY.HAS_INPUT_TEMPLATE:
                    if (object instanceof Resource) {
                        result.inputs.add(
                                loadPort(statements, (Resource) object));
                    }
                    break;
                default:
                    break;
            }
        }
        replacePorts(result.features, result.inputs);
        return result;
    }

    private static Descriptor loadDescriptor(
            List<Statement> statements, Resource resource) {
        Descriptor result = new Descriptor();
        for (Statement statement : statements) {
            if (!resource.equals(statement.getSubject())) {
                continue;
            }
            Value object = statement.getObject();
            switch (statement.getPredicate().stringValue()) {
                case DCTERMS.HAS_TITLE:
                    result.title.add(object);
                    break;
                case DISCOVERY.HAS_QUERY:
                    if (object instanceof Literal) {
                        result.query = object.stringValue();
                    }
                    break;
                case DISCOVERY.HAS_APPLIES_TO:
                    if (object instanceof Resource) {
                        result.appliesTo =
                                loadPort(statements, (Resource) object);
                    }
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private static Feature loadFeature(
            List<Statement> statements, Resource resource) {
        Feature result = new Feature();
        for (Statement statement : statements) {
            if (!resource.equals(statement.getSubject())) {
                continue;
            }
            Value object = statement.getObject();
            switch (statement.getPredicate().stringValue()) {
                case DCTERMS.HAS_TITLE:
                    result.title.add(object);
                    break;
                case DISCOVERY.HAS_DESCRIPTOR:
                    if (object instanceof Resource) {
                        result.descriptors.add(
                                loadDescriptor(statements, (Resource) object));
                    }
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private static PortTemplate loadPort(
            List<Statement> statements, Resource resource) {
        PortTemplate result = new PortTemplate();
        result.iri = resource.stringValue();
        for (Statement statement : statements) {
            if (!resource.equals(statement.getSubject())) {
                continue;
            }
            Value object = statement.getObject();
            switch (statement.getPredicate().stringValue()) {
                case DCTERMS.HAS_TITLE:
                    result.title.add(object);
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    private static Configuration loadConfiguration(
            List<Statement> statements, Resource resource) {
        Configuration result = new Configuration();
        for (Statement statement : statements) {
            if (!resource.equals(statement.getSubject())) {
                continue;
            }
            Value object = statement.getObject();
            switch (statement.getPredicate().stringValue()) {
                case DCTERMS.HAS_TITLE:
                    result.title.add(object);
                    break;
                case DISCOVERY.HAS_QUERY:
                    if (object instanceof Literal) {
                        result.query = object.stringValue();
                    }
                    break;
                case DISCOVERY.HAS_CONFIGURATION_QUERY:
                    if (object instanceof Literal) {
                        result.configurationQuery = object.stringValue();
                    }
                    break;
                default:
                    break;
            }
        }
        return result;
    }

    /**
     * Replace ports in features with provided ports.
     *
     * <p>During loading we load the same entity multiple times, by this
     * we make sure that only one port entity is used.
     */
    private static void replacePorts(
            List<Feature> features, List<PortTemplate> ports) {
        Map<String, PortTemplate> portMaps = new HashMap<>();
        ports.forEach((port) -> portMaps.put(port.iri, port));
        features.forEach((feature) -> {
            feature.descriptors.forEach((descriptor) -> {
                descriptor.appliesTo = portMaps.getOrDefault(
                        descriptor.appliesTo.iri, descriptor.appliesTo);
            });
        });
    }

    public static Transformer loadTransformer(List<Statement> statements)
            throws UnexpectedInput {
        Resource resource =
                StatementUtils.findOneByClass(statements, TRANSFORMER);
        return loadTransformer(statements, resource);
    }

    private static Transformer loadTransformer(
            List<Statement> statements, Resource resource) {
        Transformer result = new Transformer();
        result.iri = resource.stringValue();
        for (Statement statement : statements) {
            if (!resource.equals(statement.getSubject())) {
                continue;
            }
            Value object = statement.getObject();
            switch (statement.getPredicate().stringValue()) {
                case DCTERMS.HAS_TITLE:
                    result.title.add(object);
                    break;
                case DCTERMS.HAS_DESCRIPTION:
                    result.description.add(object);
                    break;
                case DISCOVERY.HAS_CONFIGURATION_TEMPLATE:
                    if (object instanceof Resource) {
                        result.configurationTemplate =
                                loadConfiguration(
                                        statements, (Resource) object);
                    }
                    break;
                case DISCOVERY.HAS_FEATURE:
                    if (object instanceof Resource) {
                        result.features.add(
                                loadFeature(statements, (Resource) object));
                    }
                    break;
                case DISCOVERY.HAS_INPUT_TEMPLATE:
                    if (object instanceof Resource) {
                        result.inputs.add(
                                loadPort(statements, (Resource) object));
                    }
                    break;
                case DISCOVERY.HAS_OUTPUT_TEMPLATE:
                    if (object instanceof Resource) {
                        result.outputs.add(
                                loadPort(statements, (Resource) object));
                    }
                    break;
                default:
                    break;
            }
        }
        replacePorts(result.features, result.inputs);
        replacePorts(result.features, result.outputs);
        return result;
    }

    public static Dataset loadDataset(String iri, File directory)
            throws IOException {
        return loadDataset(
                iri,
                RdfAdapter.asStatements(new File(directory, "sample.ttl")));
    }

    public static Dataset loadDataset(String iri, List<Statement> sample) {
        Dataset result = new Dataset();
        result.iri = iri;
        result.sample = sample;
        return result;
    }

}
