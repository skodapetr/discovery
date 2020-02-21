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
         * Starting from zero (root) denote the level in the exploration tree.
         */
        public int level;

        /**
         * Number of nodes generated on given level before filtering.
         */
        public int generated;

        /**
         * Number of nodes in the level after filtering.
         */
        public int size;

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

        /**
         * Information to store from meters.
         */
        public Map<String, Long> meters = new HashMap<>();

        public void add(Level other) {
            for (var entry : other.pipelinesPerApplication.entrySet()) {
                int value = entry.getValue()
                        + pipelinesPerApplication.getOrDefault(
                        entry.getKey(), 0);
                pipelinesPerApplication.put(entry.getKey(), value);
            }

            generated += other.generated;
            size += other.size;
            applications.addAll(other.applications);
            transformers.addAll(other.transformers);
            for (var entry : other.meters.entrySet()) {
                long value = entry.getValue()
                        + meters.getOrDefault(entry.getKey(), 0L);
                meters.put(entry.getKey(), value);
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
