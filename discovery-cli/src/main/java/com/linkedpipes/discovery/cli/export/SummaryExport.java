package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.cli.model.NamedDiscoveryStatistics;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class SummaryExport {

    private static class DiscoveryGroups extends
            HashMap<String, List<NamedDiscoveryStatistics>> {

        public DiscoveryGroups(Collection<NamedDiscoveryStatistics> items) {
            for (NamedDiscoveryStatistics item : items) {
                String key = item.discoveryIri;
                if (!containsKey(key)) {
                    put(key, new ArrayList<>());
                }
                get(key).add(item);
            }
        }

    }

    private static String[] HEADER_DISCOVERY = {
            "discovery",
            "pipelines count",
            "time (s)",
            "applications count",
            "used applications count",
            "datasets counts",
            "transformers count",
            "used transformers count",
    };

    private static String[] HEADER_DATASET_DISCOVERY = {
            "path",
            "dataset",
            "dataset sparql endpoint",
            "discovery",
            "dataset label",
            "pipelines count",
            "applications count",
    };

    private static String[] HEADER_APPLICATION_DISCOVERY = {
            "application",
            "discovery",
            "application label",
            "dataset count",
            "pipelines count",
    };

    private static String[] HEADER_DATASET_APPLICATION_DISCOVERY = {
            "path",
            "dataset",
            "dataset sparql endpoint",
            "application",
            "discovery",
            "dataset label",
            "application label",
            "pipelines count"
    };

    public static void export(
            Collection<NamedDiscoveryStatistics> statistics,
            File directory) throws IOException {
        directory.mkdirs();
        exportDiscovery(
                statistics,
                new File(directory, "discovery.csv"));
        exportDatasetDiscovery(
                statistics,
                new File(directory, "discovery-dataset.csv"));
        exportApplicationDiscovery(
                statistics,
                new File(directory, "application-discovery.csv"));
        exportDatasetApplicationDiscovery(
                statistics,
                new File(directory, "dataset-application-discovery.csv"));
    }

    private static void exportDiscovery(
            Collection<NamedDiscoveryStatistics> statistics,
            File output) throws IOException {
        DiscoveryGroups groups = new DiscoveryGroups(statistics);
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_DISCOVERY);
            for (var groupEntry : groups.entrySet()) {
                NamedDiscoveryStatistics.Level agg =
                        new NamedDiscoveryStatistics.Level();
                // We need just one discovery.
                NamedDiscoveryStatistics stats = null;
                for (var discoveryEntry : groupEntry.getValue()) {
                    stats = discoveryEntry;
                    for (var level : discoveryEntry.levels) {
                        agg.add(level);
                    }
                }
                if (stats == null) {
                    // Should not happen, just to be safe.
                    throw new RuntimeException(
                            "Empty group: " + groupEntry.getKey());
                }
                String[] row = {
                        groupEntry.getKey(),
                        Integer.toString(agg.pipelinesCount()),
                        Long.toString(agg.durationInMilliSeconds / 100),
                        Integer.toString(stats.applications.size()),
                        Integer.toString(agg.applications.size()),
                        Integer.toString(groupEntry.getValue().size()),
                        Integer.toString(stats.transformers.size()),
                        Integer.toString(agg.transformers.size())
                };
                writer.writeNext(row);
            }
        }
    }

    private static void exportDatasetDiscovery(
            Collection<NamedDiscoveryStatistics> statistics,
            File output) throws IOException {
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_DATASET_DISCOVERY);
            for (var entry : statistics) {
                NamedDiscoveryStatistics.Level agg = entry.aggregate();
                String[] row = {
                        entry.name,
                        entry.dataset.iri,
                        entry.dataset.sparqlEndpoint,
                        entry.discoveryIri,
                        entry.dataset.title.asString(),
                        Integer.toString(agg.pipelinesCount()),
                        Integer.toString(agg.applications.size()),
                };
                writer.writeNext(row);
            }
        }
    }

    private static void exportApplicationDiscovery(
            Collection<NamedDiscoveryStatistics> statistics,
            File output) throws IOException {
        DiscoveryGroups groups = new DiscoveryGroups(statistics);
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_APPLICATION_DISCOVERY);
            for (var groupEntry : groups.entrySet()) {
                NamedDiscoveryStatistics.Level agg =
                        new NamedDiscoveryStatistics.Level();
                for (var discoveryEntry : groupEntry.getValue()) {
                    agg.add(discoveryEntry.aggregate());
                }
                for (var appEntry : agg.pipelinesPerApplication.entrySet()) {
                    String[] row = {
                            appEntry.getKey().iri,
                            groupEntry.getKey(),
                            appEntry.getKey().title.asString(),
                            Integer.toString(groupEntry.getValue().size()),
                            Integer.toString(agg.pipelinesCount()),
                    };
                    writer.writeNext(row);
                }
            }
        }
    }

    private static void exportDatasetApplicationDiscovery(
            Collection<NamedDiscoveryStatistics> statistics,
            File output) throws IOException {
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_DATASET_APPLICATION_DISCOVERY);
            for (var entry : statistics) {
                NamedDiscoveryStatistics.Level agg = entry.aggregate();
                for (var appEntry : agg.pipelinesPerApplication.entrySet()) {
                    String[] row = {
                            entry.name,
                            entry.dataset.iri,
                            entry.dataset.sparqlEndpoint,
                            appEntry.getKey().iri,
                            entry.discoveryIri,
                            entry.dataset.title.asString(),
                            appEntry.getKey().title.asString(),
                            Integer.toString(agg.pipelinesCount())
                    };
                    writer.writeNext(row);
                }
            }
        }
    }

}
