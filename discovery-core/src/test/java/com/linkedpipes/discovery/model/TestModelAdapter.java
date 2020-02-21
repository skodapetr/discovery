package com.linkedpipes.discovery.model;

import com.linkedpipes.discovery.TestResources;
import com.linkedpipes.discovery.rdf.LangString;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestModelAdapter {

    @Test
    public void loadDcTermsApplication() throws UnexpectedInput {
        Application expected = new Application();
        expected.iri =
                "https://discovery.linkedpipes.com/resource/application/"
                        + "dcterms/template";
        expected.title =
                LangString.instance("en", "DCTerms Application");
        expected.description =
                LangString.instance("en", "Displays dcterms instances");
        expected.executor = "https://visualization-apps.netlify.com/dct";

        Configuration configuration =
                new Configuration();
        configuration.title =
                LangString.instance("en", "Default configuration");
        configuration.configurationQuery = "\n"
                + "PREFIX dcterms: <http://purl.org/dc/terms/>\n"
                + "PREFIX application:  "
                + "<https://discovery.linkedpipes.com/resource/application/"
                + "dcterms/>\n"
                + "PREFIX configuration-vocabulary: "
                + "<https://discovery.linkedpipes.com/vocabulary/application/"
                + "dcterms/configuration/>\n"
                + "\n"
                + "CONSTRUCT {\n"
                + "  ?config a configuration-vocabulary:Configuration ;\n"
                + "    dcterms:title ?title ;\n"
                + "} WHERE {\n"
                + "  ?config a configuration-vocabulary:Configuration .\n"
                + "  OPTIONAL { ?config dcterms:title ?title . }\n"
                + "}\n";
        expected.configurationTemplate = configuration;

        PortTemplate port = new PortTemplate();
        port.iri =
                "https://discovery.linkedpipes.com/resource/application/"
                        + "dcterms/input";
        port.title = LangString.instance("Input of DCTerms Application");
        expected.inputs.add(port);

        Feature feature = new Feature();
        feature.title = LangString.instance("The default feature");
        expected.features.add(feature);

        Descriptor descriptor = new Descriptor();
        descriptor.title =
                LangString.instance(
                        "Checks if default feature can be applied.");
        descriptor.query = "\n"
                + "PREFIX dct: <http://purl.org/dc/terms/>\n"
                + "ASK { [] dct:title [] }\n";
        descriptor.appliesTo = port;
        feature.descriptors.add(descriptor);
        //
        Application actual =
                ModelAdapter.loadApplication(
                        TestResources.asStatements("model/app-dcterms.ttl"));
        //
        Assertions.assertEquals(expected.iri, actual.iri);
        Assertions.assertEquals(expected.title, actual.title);
        Assertions.assertEquals(expected.description, actual.description);
        Assertions.assertEquals(expected.executor, actual.executor);
        Assertions.assertEquals(
                expected.configurationTemplate.title,
                actual.configurationTemplate.title);
        Assertions.assertEquals(
                expected.configurationTemplate.configurationQuery,
                actual.configurationTemplate.configurationQuery);
        Assertions.assertEquals(1, expected.inputs.size());
        Assertions.assertEquals(expected.inputs.size(), actual.inputs.size());
        Assertions.assertEquals(
                expected.inputs.get(0).title,
                actual.inputs.get(0).title);
        Assertions.assertEquals(1, expected.features.size());
        Assertions.assertEquals(
                expected.features.size(), actual.features.size());
        Assertions.assertEquals(
                expected.features.get(0).title,
                actual.features.get(0).title);
        Assertions.assertEquals(
                expected.features.get(0).descriptors.size(),
                actual.features.get(0).descriptors.size());
        Assertions.assertEquals(
                1, expected.features.get(0).descriptors.size());
        Assertions.assertEquals(
                expected.features.get(0).descriptors.get(0).title,
                actual.features.get(0).descriptors.get(0).title);
        Assertions.assertEquals(
                expected.features.get(0).descriptors.get(0).appliesTo.title,
                actual.features.get(0).descriptors.get(0).appliesTo.title);
        Assertions.assertEquals(
                expected.features.get(0).descriptors.get(0).query,
                actual.features.get(0).descriptors.get(0).query);
        Assertions.assertEquals(
                expected.inputs.get(0).iri,
                actual.inputs.get(0).iri);
        Assertions.assertEquals(
                actual.features.get(0).descriptors.get(0).appliesTo,
                actual.inputs.get(0));
    }

    @Test
    public void loadDceToDcTermTransformer() throws UnexpectedInput {
        Transformer expected = new Transformer();
        expected.iri = "https://discovery.linkedpipes.com/resource/"

                + "transformer/dce-title-to-dcterms-title/template";
        expected.title = LangString.instance(
                "en", "Dublin Core elements title to Dublin Core terms title");

        Configuration configuration = new Configuration();
        configuration.title = LangString.instance("Default configuration");
        configuration.query = "\n"
                + "PREFIX dce: <http://purl.org/dc/elements/1.1/>\n"
                + "PREFIX dct: <http://purl.org/dc/terms/>\n"
                + "\n"
                + "DELETE { ?s dce:title ?title . }\n"
                + "INSERT { ?s dct:title ?title . }\n"
                + "WHERE { ?s dce:title ?title . }\n";
        configuration.configurationQuery = "\n"
                + "PREFIX dcterms: <http://purl.org/dc/terms/>\n"
                + "PREFIX lpd: "
                + "<https://discovery.linkedpipes.com/vocabulary/>\n"
                + "PREFIX configuration-vocabulary: <"
                + "https://discovery.linkedpipes.com/vocabulary/transformer/"
                + "dce-title-to-dcterms-title/configuration/>\n"
                + "\n"
                + "CONSTRUCT {\n"
                + "  ?config a configuration-vocabulary:Configuration ;\n"
                + "    lpd:query ?query ;\n"
                + "    dcterms:title ?title .\n"
                + "} WHERE {\n"
                + "  ?config a configuration-vocabulary:Configuration .\n"
                + "  OPTIONAL { ?config lpd:query ?query . }\n"
                + "  OPTIONAL { ?config dcterms:title ?title . }\n"
                + "}\n";
        expected.configurationTemplate = configuration;
        PortTemplate input = new PortTemplate();
        input.iri = "https://discovery.linkedpipes.com/resource/transformer/"
                + "dce-title-to-dcterms-title/input";
        input.title = LangString.instance(
                "Triples with Dublin Core elements title predicate");
        expected.inputs.add(input);
        PortTemplate output = new PortTemplate();
        output.iri = "https://discovery.linkedpipes.com/resource/transformer/"
                + "dce-title-to-dcterms-title/output";
        output.title = LangString.instance(
                "Representation of objects of the input triples expressed "
                        + "as Dublin Core terms triples");
        expected.outputs.add(output);
        Feature feature = new Feature();
        feature.title = LangString.instance(
                "Transforms Dublin Core elements title"
                        + " to Dublin Core terms title");
        expected.features.add(feature);
        Descriptor descriptor = new Descriptor();
        descriptor.query = "\n"
                + "PREFIX dce: <http://purl.org/dc/elements/1.1/>\n"
                + "\n"
                + "ASK { ?s dce:title ?title . }\n";
        descriptor.appliesTo = input;
        feature.descriptors.add(descriptor);
        //
        Transformer actual =
                ModelAdapter.loadTransformer(TestResources.asStatements(
                        "model/dce-title-to-dcterms-title.ttl"));
        //
        Assertions.assertEquals(expected.iri, actual.iri);
        Assertions.assertEquals(expected.title, actual.title);
        Assertions.assertEquals(expected.description, actual.description);
        Assertions.assertEquals(
                expected.configurationTemplate.title,
                actual.configurationTemplate.title);
        Assertions.assertEquals(
                expected.configurationTemplate.query,
                actual.configurationTemplate.query);
        Assertions.assertEquals(1, expected.inputs.size());
        Assertions.assertEquals(expected.inputs.size(), actual.inputs.size());
        Assertions.assertEquals(
                expected.inputs.get(0).title,
                actual.inputs.get(0).title);
        Assertions.assertEquals(1, expected.features.size());
        Assertions.assertEquals(
                expected.features.size(), actual.features.size());
        Assertions.assertEquals(
                expected.features.get(0).title,
                actual.features.get(0).title);
        Assertions.assertEquals(
                expected.features.get(0).descriptors.size(),
                actual.features.get(0).descriptors.size());
        Assertions.assertEquals(
                1, expected.features.get(0).descriptors.size());
        Assertions.assertEquals(
                expected.features.get(0).descriptors.get(0).title,
                actual.features.get(0).descriptors.get(0).title);
        Assertions.assertEquals(
                expected.features.get(0).descriptors.get(0).appliesTo.title,
                actual.features.get(0).descriptors.get(0).appliesTo.title);
        Assertions.assertEquals(
                expected.features.get(0).descriptors.get(0).query,
                actual.features.get(0).descriptors.get(0).query);
        Assertions.assertEquals(
                1, expected.outputs.size());
        Assertions.assertEquals(
                expected.inputs.get(0).iri,
                actual.inputs.get(0).iri);
        Assertions.assertEquals(
                expected.outputs.get(0).iri,
                actual.outputs.get(0).iri);
        Assertions.assertEquals(
                actual.features.get(0).descriptors.get(0).appliesTo,
                actual.inputs.get(0));
    }

    @Test
    public void loadDataset() throws IOException {
        Dataset actual = ModelAdapter.loadDataset(
                "http://localhost/dataset",
                "Local dataset",
                TestResources.file("model/business-entities-cz-ares"));
        Assertions.assertEquals(
                "http://localhost/dataset",
                actual.iri);
        Assertions.assertEquals(9, actual.sample.size());
    }

}
