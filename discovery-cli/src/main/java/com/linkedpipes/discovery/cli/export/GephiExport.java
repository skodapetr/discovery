package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.TransformerGroup;
import com.linkedpipes.discovery.node.Node;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class GephiExport {

    private static class Edge {

        private Vertex source;

        private Vertex target;

        private String transformer;

        public Edge(Vertex source, Vertex target, String transformer) {
            this.source = source;
            this.target = target;
            this.transformer = transformer;
        }
    }

    private static class Vertex {

        private String id;

        private int level;

        private List<Boolean> applications;

        private String group;

        private boolean redundant;

        public Vertex(
                String id, int level, List<Boolean> applications,
                String group, boolean redundant) {
            this.id = id;
            this.level = level;
            this.applications = applications;
            this.group = group;
            this.redundant = redundant;
        }

    }

    private static String[] EDGE_HEADER = {"source", "target", "transformer"};

    public static void export(
            Node root, File edgeFile, File verticesFile,
            List<Application> applications,
            List<TransformerGroup> groups)
            throws IOException {
        List<Edge> edges = new ArrayList<>();
        List<Vertex> vertices = new ArrayList<>();
        //
        Map<Node, Vertex> vertexMap = new HashMap<>();
        Deque<Node> nodes = new ArrayDeque<>();
        nodes.add(root);
        while (!nodes.isEmpty()) {
            Node node = nodes.pop();
            nodes.addAll(node.getNext());
            // Create vertex.
            Vertex vertex = new Vertex(
                    node.getId(),
                    node.getLevel(),
                    collectApps(node, applications),
                    getGroup(node, groups),
                    node.isRedundant());
            vertexMap.put(node, vertex);
            vertices.add(vertex);
            // Add edge to parent.
            if (node.getPrevious() != null) {
                Vertex prev = vertexMap.get(node.getPrevious());
                edges.add(new Edge(prev, vertex, node.getTransformer().iri));
            }
        }
        //
        writeEdgeFile(edges, edgeFile);
        writeVerticesFile(vertices, verticesFile, applications);
    }

    private static List<Boolean> collectApps(
            Node node, List<Application> applications) {
        return applications.stream()
                .map((app) -> node.getApplications().contains(app))
                .collect(Collectors.toList());
    }

    private static String getGroup(Node node, List<TransformerGroup> groups) {
        if (node.getTransformer() == null) {
            return "";
        }
        String transformer = node.getTransformer().iri;
        for (TransformerGroup group : groups) {
            if (group.transformers.contains(transformer)) {
                return group.iri;
            }
        }
        return "";
    }

    private static void writeEdgeFile(List<Edge> edges, File file)
            throws IOException {
        file.getParentFile().mkdirs();
        try (var printWriter = new PrintWriter(file, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(EDGE_HEADER);
            for (Edge edge : edges) {
                String[] row = {
                        edge.source.id,
                        edge.target.id,
                        edge.transformer
                };
                writer.writeNext(row);
            }
        }
    }

    private static void writeVerticesFile(
            List<Vertex> vertices, File file,
            List<Application> applications)
            throws IOException {
        file.getParentFile().mkdirs();
        List<String> header = new ArrayList<>();
        header.addAll(Arrays.asList("id", "level", "redundant", "group"));
        for (Application application : applications) {
            header.add(application.title.asString());
        }
        //
        try (var printWriter = new PrintWriter(file, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(header.toArray(new String[0]));
            for (Vertex vertex : vertices) {
                List<String> row = new ArrayList<>();
                row.add(vertex.id);
                row.add(Integer.toString(vertex.level));
                row.add(vertex.redundant ? "1" : "0");
                row.add(vertex.group);
                for (Boolean app : vertex.applications) {
                    row.add(app ? "1" : "0");
                }
                writer.writeNext(row.toArray(new String[0]));
            }
        }
    }

}

