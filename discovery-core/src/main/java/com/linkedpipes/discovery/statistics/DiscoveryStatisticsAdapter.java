package com.linkedpipes.discovery.statistics;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.rdf.LangString;

import java.io.File;
import java.io.IOException;

public class DiscoveryStatisticsAdapter {

    public boolean statisticsSaved(File directory) {
        return getDiscoveryFile(directory).exists();
    }

    private File getDiscoveryFile(File directory) {
        return new File(directory, "statistics.json");
    }

    public void save(DiscoveryStatistics statistics, File directory)
            throws DiscoveryException {
        ObjectMapper objectMapper = new ObjectMapper();

        ObjectNode root = objectMapper.createObjectNode();
        root.put("iri", statistics.discoveryIri);

        ObjectNode dataset = objectMapper.createObjectNode();
        dataset.put("iri", statistics.dataset.iri);
        ObjectNode datasetTitle = objectMapper.createObjectNode();
        for (var entry : statistics.dataset.title.getValues().entrySet()) {
            datasetTitle.put(entry.getKey(), entry.getValue());
        }
        dataset.set("title", datasetTitle);
        root.set("dataset", dataset);

        ArrayNode levels = objectMapper.createArrayNode();
        root.set("level", levels);

        for (DiscoveryStatistics.Level level : statistics.levels) {
            ObjectNode levelNode = objectMapper.createObjectNode();
            levels.add(levelNode);
            levelNode.put("duration", level.durationInSeconds);
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

            ObjectNode pipelines = objectMapper.createObjectNode();
            levelNode.set("pipelines", transformers);
            for (var pipeline : level.pipelinesPerApplication.entrySet()) {
                pipelines.put(pipeline.getKey().iri, pipeline.getValue());
            }
        }

        try {
            objectMapper.writeValue(getDiscoveryFile(directory), root);
        } catch (IOException ex) {
            throw new DiscoveryException("Can't save statistics.", ex);
        }
    }

    public DiscoveryStatistics load(Discovery discovery, File directory)
            throws DiscoveryException {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode root;
        try {
            root = (ObjectNode) objectMapper.readTree(
                    getDiscoveryFile(directory));
        } catch (IOException ex) {
            throw new DiscoveryException("Can't load statistics.", ex);
        }
        DiscoveryStatistics result = new DiscoveryStatistics();
        result.discoveryIri = root.textNode("iri").textValue();

        ObjectNode datasetNode = (ObjectNode) root.get("dataset");
        LangString datasetLang = new LangString();
        ObjectNode datasetLangNode = (ObjectNode) datasetNode.get("title");
        var iterator = datasetLangNode.fields();
        while (iterator.hasNext()) {
            var item = iterator.next();
            datasetLang.add(item.getKey(), item.getValue().textValue());
        }

        DiscoveryStatistics.DatasetRef datasetRef =
                new DiscoveryStatistics.DatasetRef(
                        datasetNode.textNode("iri").textValue(),
                        datasetLang);
        result.dataset = datasetRef;

        ArrayNode levels = (ArrayNode) root.get("level");
        for (JsonNode levelNode : levels) {
            DiscoveryStatistics.Level level = new DiscoveryStatistics.Level();
            result.levels.add(level);
            level.durationInSeconds = levelNode.get("duration").asInt();
            level.level = levelNode.get("level").asInt();
            level.startNodes = levelNode.get("startNodes").asInt();
            level.expandedNodes = levelNode.get("expandedNodes").asInt();
            level.filteredNodes = levelNode.get("filteredNodes").asInt();
            level.newNodes = levelNode.get("newNodes").asInt();
            level.nextLevel = levelNode.get("nextLevel").asInt();
            ArrayNode applicationsNode =
                    (ArrayNode) levelNode.get("application");
            for (JsonNode node : applicationsNode) {
                level.applications.add(
                        getApplication(discovery, node.asText()));
            }

            ArrayNode transformersNode =
                    (ArrayNode) levelNode.get("transformer");
            for (JsonNode node : transformersNode) {
                level.transformers.add(
                        getTransformer(discovery, node.asText()));
            }

            ObjectNode pipelineNode = (ObjectNode) levelNode.get("pipelines");
            var pipelineIterator = pipelineNode.fields();
            while (pipelineIterator.hasNext()) {
                var pipelineEntry = pipelineIterator.next();
                level.pipelinesPerApplication.put(
                        getApplication(discovery, pipelineEntry.getKey()),
                        pipelineEntry.getValue().intValue());

            }

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
