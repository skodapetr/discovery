package com.linkedpipes.discovery.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import com.linkedpipes.discovery.sample.store.SampleGroup;
import com.linkedpipes.discovery.sample.store.SampleRef;
import com.linkedpipes.discovery.sample.store.SampleStore;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DiscoveryAdapter {

    private static class NodeIoContainer {

        public String name;

        public String transformer;

        public List<String> applications;

        public String previous;

        public List<String> next;

        public int level;

        public String dataSampleRef;

        public boolean redundant;

        public boolean expanded;

    }

    public void save(Discovery context, File directory)
            throws DiscoveryException {
        directory = new File(directory, "resume-data");
        directory.mkdirs();
        try {
            var refMap = saveStore(context.getStore(), directory);
            saveFilter(context, directory, refMap);
            saveNodes(context, directory, refMap);
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save discovery context.", ex);
        }
    }

    /**
     * We need to create a map from {@link SampleRef} to {@link String}.
     * In order to do so we need to iterate all entries, by doing so we
     * load the statements.
     * So as we have all we need we save the statements to the file.
     */
    private Map<SampleRef, String> saveStore(
            SampleStore store, File directory) throws IOException {
        directory = getStatementsDirectory(directory);
        directory.mkdirs();
        var iterator = store.iterator();
        int counter = 0;
        Map<SampleRef, String> result = new HashMap<>();
        while (iterator.hasNext()) {
            SampleStore.Entry next = iterator.next();
            String name =
                    "ref-" + next.ref.getGroup().name() + "-" + (++counter);
            result.put(next.ref, name);
            File file = new File(directory, name + ".nt");
            RdfAdapter.toFile(next.statements, file);
        }
        return result;
    }

    private File getStatementsDirectory(File directory) {
        return new File(directory, "statements");
    }

    private void saveFilter(
            Discovery context, File directory,
            Map<SampleRef, String> refMap)
            throws IOException, DiscoveryException {
        context.getFilter().save(directory, refMap::get);
    }

    private void saveNodes(
            Discovery context, File directory,
            Map<SampleRef, String> refMap) throws IOException {
        Map<Node, String> nodeToString = new HashMap<>();
        context.getRoot().accept(node -> {
            nodeToString.put(node, "node-" + nodeToString.size());
        });
        List<NodeIoContainer> nodes = new ArrayList<>(nodeToString.size());
        context.getRoot().accept(node -> {
            NodeIoContainer container = new NodeIoContainer();
            container.name = nodeToString.get(node);
            if (node.getTransformer() != null) {
                container.transformer = node.getTransformer().iri;
            }
            container.applications = node.getApplications().stream()
                    .map(app -> app.iri)
                    .collect(Collectors.toList());
            container.previous = nodeToString.get(node.getPrevious());
            container.next = node.getNext().stream()
                    .map(nodeToString::get)
                    .collect(Collectors.toList());
            container.level = node.getLevel();
            container.dataSampleRef = refMap.get(node.getDataSampleRef());
            container.redundant = node.isRedundant();
            container.expanded = node.isExpanded();
            nodes.add(container);
        });
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(getNodesFile(directory), nodes);
        List<String> queue = context.getQueue().stream()
                .map(nodeToString::get)
                .collect(Collectors.toList());
        objectMapper.writeValue(getQueueFile(directory), queue);
    }

    private File getNodesFile(File directory) {
        return new File(directory, "nodes.json");
    }


    private File getQueueFile(File directory) {
        return new File(directory, "nodes-queue.json");
    }

    public void load(File directory, Discovery context)
            throws DiscoveryException {
        directory = new File(directory, "resume-data");
        try {
            var refMap = loadStore(context.getStore(), directory);
            loadFilter(context, directory, refMap);
            loadNodes(context, directory, refMap);
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save discovery context.", ex);
        }
    }

    private Map<String, SampleRef> loadStore(SampleStore store, File directory)
            throws IOException, DiscoveryException {
        directory = getStatementsDirectory(directory);
        File[] files = directory.listFiles();
        if (files == null) {
            return new HashMap<>();
        }
        Map<String, SampleRef> result = new HashMap<>();
        for (File file : files) {
            List<Statement> statements = RdfAdapter.asStatements(file);
            String fileName = file.getName();
            String[] name = fileName.split("-");
            SampleRef ref =
                    store.store(statements, SampleGroup.valueOf(name[1]));
            String refName = fileName.substring(0, fileName.indexOf("."));
            result.put(refName, ref);
        }
        return result;
    }

    private void loadFilter(
            Discovery context, File directory,
            Map<String, SampleRef> refMap)
            throws IOException, DiscoveryException {
        context.getFilter().load(directory, refMap::get);
    }

    private void loadNodes(
            Discovery context, File directory,
            Map<String, SampleRef> refMap) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        NodeIoContainer[] nodes = objectMapper.readValue(
                getNodesFile(directory), NodeIoContainer[].class);
        Map<String, Node> nodeMap = new HashMap<>();
        // Load nodes - they are saved so we always have parents ready
        // before loading children.
        Node root = null;
        for (NodeIoContainer nodeContainer : nodes) {
            if (nodeContainer.previous == null) {
                root = new Node();
                root.setNext(new ArrayList<>());
                root.setDataSampleRef(refMap.get(nodeContainer.dataSampleRef));
                root.setExpanded(nodeContainer.expanded);
                root.setApplications(getApplications(context, nodeContainer));
                root.setRedundant(nodeContainer.redundant);
                nodeMap.put(nodeContainer.name, root);
                continue;
            }
            Node parent = nodeMap.get(nodeContainer.previous);
            Transformer transformer = getTransformer(context, nodeContainer);
            Node node = new Node(parent, transformer);
            node.setNext(new ArrayList<>());
            parent.addNext(node);
            //
            node.setDataSampleRef(refMap.get(nodeContainer.dataSampleRef));
            node.setExpanded(nodeContainer.expanded);
            node.setApplications(getApplications(context, nodeContainer));
            node.setRedundant(nodeContainer.redundant);
            nodeMap.put(nodeContainer.name, node);
        }
        context.setRoot(root);
        // Select queue.
        String[] queue = objectMapper.readValue(
                getQueueFile(directory), String[].class);
        for (String ref : queue) {
            context.getQueue().push(nodeMap.get(ref));
        }

    }

    private List<Application> getApplications(
            Discovery context, NodeIoContainer container) {
        return context.getApplications().stream()
                .filter(app -> container.applications.contains(app.iri))
                .collect(Collectors.toList());
    }

    private Transformer getTransformer(
            Discovery context, NodeIoContainer container) {
        for (Transformer transformer : context.getTransformers()) {
            if (transformer.iri.equals(container.transformer)) {
                return transformer;
            }
        }
        return null;
    }

}
