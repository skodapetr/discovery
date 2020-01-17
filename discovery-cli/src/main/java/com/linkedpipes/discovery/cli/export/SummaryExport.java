package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.rdf.ExplorerStatistics;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SummaryExport {

    private static String[] HEADER =
            {"data source", "#pipelines", "#nodes", "#applications"};

    public static void export(
            Map<String, ExplorerStatistics> statistics, File output)
            throws IOException {
        output.getParentFile().mkdirs();
        try (var printWriter = new PrintWriter(output, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(HEADER);
            for (var entry : statistics.entrySet()) {
                ExplorerStatistics stats = entry.getValue();
                String[] row = {
                        entry.getKey(),
                        Integer.toString(stats.pipelines),
                        Integer.toString(stats.finalSize),
                        Integer.toString(stats.applications.size())
                };
                writer.writeNext(row);
            }
        }
    }

}
