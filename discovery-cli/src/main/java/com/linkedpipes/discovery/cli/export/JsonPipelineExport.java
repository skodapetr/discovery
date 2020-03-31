package com.linkedpipes.discovery.cli.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.node.Node;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Export all given nodes, any reduction of redundant nodes must be
 * done in caller component.
 */
public class JsonPipelineExport {

    private static class ComponentContainer {

        public final String node;

        public final String iri;

        public final String label;

        public ComponentContainer(String node, String iri, String label) {
            this.node = node;
            this.iri = iri;
            this.label = label;
        }

    }

    private static class PipelineContainer {

        public final List<ComponentContainer> components;

        public PipelineContainer(List<ComponentContainer> components) {
            this.components = components;
        }

    }

    public static void export(
            Discovery discovery, Dataset dataset, File outputFile)
            throws IOException {
        List<PipelineContainer> result = new ArrayList<>();
        discovery.getRoot().accept(node -> {
            result.addAll(nodeToPipelines(
                    dataset, discovery.getRoot(), node));
        });
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        try (var writer = new PrintWriter(outputFile, StandardCharsets.UTF_8)) {
            objectMapper.writeValue(writer, result);
        }
    }

    private static List<PipelineContainer> nodeToPipelines(
            Dataset dataset, Node root, Node node) {
        if (node.getApplications().isEmpty()) {
            return Collections.emptyList();
        }
        List<PipelineContainer> result = new ArrayList<>();
        List<ComponentContainer> transformers = collectTransformers(node);
        for (Application application : node.getApplications()) {
            List<ComponentContainer> components = new ArrayList<>();
            components.add(new ComponentContainer(
                    root.getId(),
                    dataset.iri,
                    dataset.title.asString()));
            components.addAll(transformers);
            components.add(new ComponentContainer(
                    node.getId(),
                    application.iri,
                    application.title.asString()));
            result.add(new PipelineContainer(components));
        }
        return result;
    }

    public static List<ComponentContainer> collectTransformers(Node node) {
        List<ComponentContainer> result = new ArrayList<>(node.getLevel());
        while (node != null) {
            if (node.getTransformer() != null) {
                result.add(new ComponentContainer(
                        node.getId(),
                        node.getTransformer().iri,
                        node.getTransformer().title.asString()));
            }
            node = node.getPrevious();
        }
        Collections.reverse(result);
        return result;
    }


}
