package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.cli.model.DiscoveryStatisticsInPath;
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
            HashMap<String, List<DiscoveryStatisticsInPath>> {

        public DiscoveryGroups(Collection<DiscoveryStatisticsInPath> items) {
            for (DiscoveryStatisticsInPath item : items) {
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
            "datasets counts",
            "transformers count",
    };

    private static String[] HEADER_DATASET_DISCOVERY = {
            "path",
            "dataset",
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
            "application",
            "discovery",
            "dataset label",
            "application label",
            "pipelines count",
    };

    public static void export(
            Collection<DiscoveryStatisticsInPath> statistics,
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
            Collection<DiscoveryStatisticsInPath> statistics,
            File output) throws IOException {
        DiscoveryGroups groups = new DiscoveryGroups(statistics);
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_DISCOVERY);
            for (var groupEntry : groups.entrySet()) {
                DiscoveryStatisticsInPath.Level stats =
                        new DiscoveryStatisticsInPath.Level();
                for (var discoveryEntry : groupEntry.getValue()) {
                    for (var level : discoveryEntry.levels) {
                        stats.add(level);
                    }
                }
                String[] row = {
                        groupEntry.getKey(),
                        Integer.toString(stats.pipelinesCount()),
                        Long.toString(stats.durationInSeconds()),
                        Integer.toString(stats.applications.size()),
                        Integer.toString(groupEntry.getValue().size()),
                        Integer.toString(stats.transformers.size())
                };
                writer.writeNext(row);
            }
        }
    }

    private static void exportDatasetDiscovery(
            Collection<DiscoveryStatisticsInPath> statistics,
            File output) throws IOException {
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_DATASET_DISCOVERY);
            for (var entry : statistics) {
                DiscoveryStatisticsInPath.Level agg = entry.aggregate();
                String[] row = {
                        entry.path,
                        entry.dataset.iri,
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
            Collection<DiscoveryStatisticsInPath> statistics,
            File output) throws IOException {
        DiscoveryGroups groups = new DiscoveryGroups(statistics);
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_APPLICATION_DISCOVERY);
            for (var groupEntry : groups.entrySet()) {
                DiscoveryStatisticsInPath.Level agg =
                        new DiscoveryStatisticsInPath.Level();
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
            Collection<DiscoveryStatisticsInPath> statistics,
            File output) throws IOException {
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER_DATASET_APPLICATION_DISCOVERY);
            for (var entry : statistics) {
                DiscoveryStatisticsInPath.Level agg = entry.aggregate();
                for (var appEntry : agg.pipelinesPerApplication.entrySet()) {
                    String[] row = {
                            entry.path,
                            entry.dataset.iri,
                            appEntry.getKey().iri,
                            entry.discoveryIri,
                            entry.dataset.title.asString(),
                            appEntry.getKey().title.asString(),
                            Integer.toString(agg.pipelinesCount()),
                    };
                    writer.writeNext(row);
                }
            }
        }
    }

}
