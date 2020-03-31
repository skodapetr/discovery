package com.linkedpipes.discovery.cli.experiment;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.cli.pipeline.Pipeline;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.statistics.Statistics;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

abstract class CsvFile {

    public void write(File directory) throws IOException {
        File file = new File(directory, getFileName());
        try (var printWriter = new PrintWriter(file, StandardCharsets.UTF_8);
                var writer = new CSVWriter(printWriter, ',', '"', '\\', "\n")) {
            writer.writeNext(getHeader());
            writeLines(writer);
        }
    }

    protected abstract String getFileName();

    protected abstract String[] getHeader();

    protected abstract void writeLines(CSVWriter writer);

    public abstract void add(
            String path,
            Discovery discovery, Dataset dataset, Statistics statistics,
            List<Pipeline> pipelines);

}
