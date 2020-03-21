package com.linkedpipes.discovery;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.rdf.LangString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressFBWarnings(value = {
        "UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD",
        "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
public class DiscoveryStatistics {

    public static class DatasetRef {

        public final String iri;

        public final LangString title;

        public DatasetRef(Dataset dataset) {
            iri = dataset.iri;
            title = dataset.title;
        }

    }

    public static class Level {

        /**
         * Duration spend on discovery.
         */
        public int durationInSeconds;

        /**
         * Starting from zero (root) denote the level in the exploration tree.
         */
        public int level;

        /**
         * Number of nodes on this level at the start of the exploration.
         */
        public int startNodes;

        /**
         * Number of nodes expanded on this level.
         */
        public int expandedNodes;

        /**
         * Number of filtered nodes on this level.
         */
        public int filteredNodes;

        /**
         * Number of new nodes on this level, i.e. expanded and not filtered.
         * When level is finished hold number of nodes after filtering.
         */
        public int newNodes;

        /**
         * Number of nodes generated for next level.
         */
        public int nextLevel;

        /**
         * Number of pipelines, i.e. count of all applications on all nodes
         * in this level.
         */
        public Map<Application, Integer>
                pipelinesPerApplication = new HashMap<>();

        /**
         * Matching application in the level.
         */
        public Set<Application> applications = new HashSet<>();

        /**
         * All Matching transformers in the level.
         */
        public Set<Transformer> transformers = new HashSet<>();

        public void add(Level other) {
            durationInSeconds += other.durationInSeconds;
            startNodes = 0;
            expandedNodes += other.expandedNodes;
            filteredNodes += other.filteredNodes;
            newNodes += other.newNodes;
            nextLevel = 0; // Make no sense on merge.
            applications.addAll(other.applications);
            transformers.addAll(other.transformers);
            //
            for (var entry : other.pipelinesPerApplication.entrySet()) {
                int value = entry.getValue()
                        + pipelinesPerApplication.getOrDefault(
                        entry.getKey(), 0);
                pipelinesPerApplication.put(entry.getKey(), value);
            }
        }

        /**
         * Total number of all pipelines for all applications.
         */
        public int pipelinesCount() {
            return pipelinesPerApplication.values()
                    .stream()
                    .reduce(Integer::sum)
                    .orElseGet(() -> 0);
        }

    }

    /**
     * Statistic on each level of exploration.
     */
    public List<Level> levels = new ArrayList<>();

    public String discoveryIri;

    public DatasetRef dataset;

    public Level aggregate() {
        Level result = new Level();
        result.level = -1;
        levels.forEach(result::add);
        return result;
    }

}
