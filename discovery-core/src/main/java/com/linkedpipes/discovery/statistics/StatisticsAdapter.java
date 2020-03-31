package com.linkedpipes.discovery.statistics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;

import java.io.File;
import java.io.IOException;

public class StatisticsAdapter {

    public static boolean statisticsSaved(File directory) {
        return getDiscoveryFile(directory).exists();
    }

    private static File getDiscoveryFile(File directory) {
        return new File(directory, "statistics.json");
    }

    public void save(Statistics statistics, File directory)
            throws DiscoveryException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root = save(statistics, objectMapper);
        try {
            objectMapper.writeValue(getDiscoveryFile(directory), root);
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save statistics.", ex);
        }
    }

    public ObjectNode save(
            Statistics statistics, ObjectMapper objectMapper) {
        ObjectNode root = objectMapper.createObjectNode();
        root.put("iri", statistics.discoveryIri);
        root.set("level", createLevelsNode(objectMapper, statistics));
        return root;
    }

    private ArrayNode createLevelsNode(
            ObjectMapper objectMapper, Statistics statistics) {
        ArrayNode result = objectMapper.createArrayNode();
        for (Statistics.Level level : statistics.levels) {
            ObjectNode levelNode = objectMapper.createObjectNode();
            result.add(levelNode);
            levelNode.put("duration", level.durationInMilliSeconds);
            levelNode.put("level", level.level);
            levelNode.put("startNodes", level.startNodes);
            levelNode.put("expandedNodes", level.expandedNodes);
            levelNode.put("filteredNodes", level.filteredNodes);
            levelNode.put("newNodes", level.newNodes);
            levelNode.put("nextLevel", level.nextLevel);

            ArrayNode applications = objectMapper.createArrayNode();
            levelNode.set("application", applications);
            for (Application application : level.applications) {
                applications.add(application.iri);
            }

            ArrayNode transformers = objectMapper.createArrayNode();
            levelNode.set("transformer", transformers);
            for (Transformer transformer : level.transformers) {
                transformers.add(transformer.iri);
            }

        }
        return result;
    }

    public Statistics load(Discovery discovery, File directory)
            throws DiscoveryException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root;
        try {
            root = (ObjectNode) objectMapper.readTree(
                    getDiscoveryFile(directory));
        } catch (IOException ex) {
            throw new DiscoveryException("Can't load statistics.", ex);
        }
        return load(discovery, root);
    }

    public Statistics load(Discovery discovery, ObjectNode root) {
        Statistics result = new Statistics();
        result.discoveryIri = root.get("iri").textValue();
        for (JsonNode levelNode : root.get("level")) {
            result.levels.add(loadLevel(discovery, levelNode));
        }
        return result;
    }

    private Statistics.Level loadLevel(
            Discovery discovery, JsonNode levelNode) {
        Statistics.Level result = new Statistics.Level();
        result.durationInMilliSeconds = levelNode.get("duration").asInt();
        result.level = levelNode.get("level").asInt();
        result.startNodes = levelNode.get("startNodes").asInt();
        result.expandedNodes = levelNode.get("expandedNodes").asInt();
        result.filteredNodes = levelNode.get("filteredNodes").asInt();
        result.newNodes = levelNode.get("newNodes").asInt();
        result.nextLevel = levelNode.get("nextLevel").asInt();
        ArrayNode applicationsNode =
                (ArrayNode) levelNode.get("application");
        for (JsonNode node : applicationsNode) {
            result.applications.add(
                    getApplication(discovery, node.asText()));
        }

        ArrayNode transformersNode =
                (ArrayNode) levelNode.get("transformer");
        for (JsonNode node : transformersNode) {
            result.transformers.add(
                    getTransformer(discovery, node.asText()));
        }

        return result;
    }

    private Application getApplication(Discovery discovery, String iri) {
        for (Application application : discovery.getApplications()) {
            if (application.iri.equals(iri)) {
                return application;
            }
        }
        return null;
    }

    private Transformer getTransformer(Discovery discovery, String iri) {
        for (Transformer transformer : discovery.getTransformers()) {
            if (transformer.iri.equals(iri)) {
                return transformer;
            }
        }
        return null;
    }

}
