package com.linkedpipes.discovery.cli.export;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.node.Node;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JsonExport {

    private static class Component {

        public final String node;

        public final String iri;

        public final String label;

        public Component(String node, String iri, String label) {
            this.node = node;
            this.iri = iri;
            this.label = label;
        }

    }

    private static class Pipeline {

        public final List<Component> components;

        public Pipeline(List<Component> components) {
            this.components = components;
        }

    }

    private static class OutputJson {

        public final List<Pipeline> pipelines;

        public OutputJson(List<Pipeline> pipelines) {
            this.pipelines = pipelines;
        }

    }

    public static void export(
            Discovery discovery, Node root, File outputFile,
            NodeToName nodeToName)
            throws IOException {
        List<Pipeline> pipelines = new ArrayList<>();
        root.accept((node) -> {
            pipelines.addAll(nodeToPipelines(discovery, nodeToName, node));
        });
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        OutputJson outputJson = new OutputJson(pipelines);
        try (var writer = new PrintWriter(outputFile, StandardCharsets.UTF_8)) {
            objectMapper.writeValue(writer, outputJson);
        }
    }

    private static List<Pipeline> nodeToPipelines(
            Discovery discovery, NodeToName nodeToName, Node finalNode) {
        if (finalNode.getApplications().isEmpty()) {
            return Collections.emptyList();
        }
        List<Node> nodes = collectNodes(finalNode);
        List<Component> components = new ArrayList<>();
        // Root as a data source.
        components.add(new Component(
                nodeToName.name(nodes.get(0)),
                discovery.getDataset().iri,
                "Data source"));
        nodes.remove(0);
        // Transformers.
        for (Node node : nodes) {
            components.add(new Component(
                    nodeToName.name(node),
                    node.getTransformer().iri,
                    node.getTransformer().title.asString()));
        }
        // Applications.
        return finalNode.getApplications().stream()
                .map((app -> {
                    List<Component> pipeline = new ArrayList<>();
                    pipeline.addAll(components);
                    pipeline.add(new Component(
                            nodeToName.name(finalNode),
                            app.iri,
                            app.title.asString()));
                    return new Pipeline(pipeline);
                }))
                .collect(Collectors.toList());
    }

    private static List<Node> collectNodes(Node node) {
        List<Node> result = new ArrayList<>();
        Node prev = node;
        while (prev != null) {
            if (prev.getTransformer() != null) {
                result.add(prev);
            }
            prev = prev.getPrevious();
        }
        Collections.reverse(result);
        return result;
    }


}
