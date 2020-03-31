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

        public String id;

        public String transformer;

        public List<String> applications;

        public String previous;

        public List<String> next;

        public int level;

        public String dataSampleRef;

        public boolean redundant;

        public boolean expanded;

    }

    public void saveForResume(Discovery discovery, File directory)
            throws DiscoveryException {
        directory = getResumeDirectory(directory);
        directory.mkdirs();
        try {
            var refMap = saveStore(discovery.getStore(), directory);
            saveFilter(discovery, directory, refMap);
            saveNodes(discovery, directory, refMap);
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save discovery discovery.", ex);
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
            Discovery discovery, File directory,
            Map<SampleRef, String> refMap)
            throws IOException, DiscoveryException {
        discovery.getFilter().save(directory, refMap::get);
    }

    private void saveNodes(
            Discovery discovery, File directory,
            Map<SampleRef, String> refMap) throws IOException {
        List<NodeIoContainer> nodes = new ArrayList<>();
        discovery.getRoot().accept(node -> {
            NodeIoContainer container = new NodeIoContainer();
            container.id = node.getId();
            if (node.getTransformer() != null) {
                container.transformer = node.getTransformer().iri;
            }
            container.applications = node.getApplications().stream()
                    .map(app -> app.iri)
                    .collect(Collectors.toList());
            if (node.getPrevious() != null) {
                container.previous = node.getPrevious().getId();
            }
            container.next = node.getNext().stream()
                    .map(Node::getId)
                    .collect(Collectors.toList());
            container.level = node.getLevel();
            container.dataSampleRef = refMap.get(node.getDataSampleRef());
            container.redundant = node.isRedundant();
            container.expanded = node.isExpanded();
            nodes.add(container);
        });
        directory.mkdirs();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(getNodesFile(directory), nodes);
        List<String> queue = discovery.getQueue().stream()
                .map(Node::getId)
                .collect(Collectors.toList());
        objectMapper.writeValue(getQueueFile(directory), queue);
    }

    private File getNodesFile(File directory) {
        return new File(directory, "nodes.json");
    }

    private File getQueueFile(File directory) {
        return new File(directory, "nodes-queue.json");
    }

    private File getResumeDirectory(File directory) {
        return new File(directory, "resume-data");
    }

    /**
     * Can't be used to resume the execution.
     */
    public void saveFinishedDiscovery(Discovery discovery, File directory)
            throws DiscoveryException {
        directory.mkdirs();
        try {
            saveNodes(discovery, directory, new HashMap<>());
            getFinishStatusFile(directory).createNewFile();
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save nodes.", ex);
        }
    }

    private File getFinishStatusFile(File directory) {
        return new File(directory, "discovery-finished");
    }

    public void loadFromResume(File directory, Discovery discovery)
            throws DiscoveryException {
        directory = getResumeDirectory(directory);
        try {
            var refMap = loadStore(discovery.getStore(), directory);
            loadFilter(discovery, directory, refMap);
            loadNodes(discovery, directory, refMap);
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save discovery discovery.", ex);
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
            Discovery discovery, File directory,
            Map<String, SampleRef> refMap)
            throws IOException, DiscoveryException {
        discovery.getFilter().load(directory, refMap::get);
    }

    private void loadNodes(
            Discovery discovery, File directory,
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
                root = new Node(nodeContainer.id);
                root.setNext(new ArrayList<>());
                root.setDataSampleRef(refMap.get(nodeContainer.dataSampleRef));
                root.setExpanded(nodeContainer.expanded);
                root.setApplications(getApplications(discovery, nodeContainer));
                root.setRedundant(nodeContainer.redundant);
                nodeMap.put(nodeContainer.id, root);
                continue;
            }
            Node parent = nodeMap.get(nodeContainer.previous);
            Transformer transformer = getTransformer(discovery, nodeContainer);
            Node node = new Node(nodeContainer.id, parent, transformer);
            node.setNext(new ArrayList<>());
            parent.addNext(node);
            //
            node.setDataSampleRef(refMap.get(nodeContainer.dataSampleRef));
            node.setExpanded(nodeContainer.expanded);
            node.setApplications(getApplications(discovery, nodeContainer));
            node.setRedundant(nodeContainer.redundant);
            nodeMap.put(nodeContainer.id, node);
        }
        discovery.setRoot(root);
        // Select queue.
        String[] queue = objectMapper.readValue(
                getQueueFile(directory), String[].class);
        for (String ref : queue) {
            discovery.getQueue().push(nodeMap.get(ref));
        }

    }

    private List<Application> getApplications(
            Discovery discovery, NodeIoContainer container) {
        return discovery.getApplications().stream()
                .filter(app -> container.applications.contains(app.iri))
                .collect(Collectors.toList());
    }

    private Transformer getTransformer(
            Discovery discovery, NodeIoContainer container) {
        for (Transformer transformer : discovery.getTransformers()) {
            if (transformer.iri.equals(container.transformer)) {
                return transformer;
            }
        }
        return null;
    }

    public boolean isDiscoveryFinishDataSaved(File directory) {
        return getFinishStatusFile(directory).exists();
    }

    public void loadFromFinished(File directory, Discovery discovery)
            throws DiscoveryException {
        try {
            loadNodes(discovery, directory, new HashMap<>());
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save discovery discovery.", ex);
        }
    }

}
