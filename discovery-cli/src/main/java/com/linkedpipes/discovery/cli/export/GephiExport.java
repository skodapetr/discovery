package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.node.Node;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
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

        public Vertex(String id, int level, List<Boolean> applications) {
            this.id = id;
            this.level = level;
            this.applications = applications;
        }

    }

    public static void export(
            Node root, File edgeFile, File verticesFile,
            List<Application> applications)
            throws IOException {
        List<Edge> edges = new ArrayList<>();
        List<Vertex> vertices = new ArrayList<>();
        //
        int counter = 0;
        Map<Node, Vertex> vertexMap = new HashMap<>();
        Deque<Node> nodes = new ArrayDeque<>();
        nodes.add(root);
        while (!nodes.isEmpty()) {
            Node node = nodes.pop();
            nodes.addAll(node.getNext());
            // Create vertex.
            Vertex vertex = new Vertex(
                    "node_" + counter++,
                    node.getLevel(),
                    collectApps(node, applications));
            vertexMap.put(node, vertex);
            vertices.add(vertex);
            // Add edge to parent.
            if (node.getPrevious() != null) {
                Vertex prev = vertexMap.get(node.getPrevious());
                edges.add(new Edge(
                        prev, vertex,
                        node.getTransformer().title.asString()));

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

    private static void writeEdgeFile(List<Edge> edges, File file)
            throws IOException {
        file.getParentFile().mkdirs();
        try (var writer = new PrintWriter(file, StandardCharsets.UTF_8)) {
            writer.write("\"source\",\"target\",\"transformer\"\n");
            for (Edge edge : edges) {
                writer.write("\""
                        + edge.source.id + "\",\""
                        + edge.target.id + "\",\""
                        + edge.transformer + "\"\n");
            }
        }
    }

    private static void writeVerticesFile(
            List<Vertex> vertices, File file, List<Application> applications)
            throws IOException {
        file.getParentFile().mkdirs();
        try (var writer = new PrintWriter(file, StandardCharsets.UTF_8)) {
            writer.write("\"id\",\"level\"");
            for (Application application : applications) {
                writer.write(",\"" + application.title.asString() + "\"");
            }
            writer.write("\n");
            for (Vertex vertex : vertices) {
                writer.write("\"" + vertex.id + "\",\"" + vertex.level + "\"");
                for (Boolean app : vertex.applications) {
                    if (app) {
                        writer.write(",\"1\"");
                    } else {
                        writer.write(",\"0\"");
                    }
                }
                writer.write("\n");
            }
        }
    }

}
