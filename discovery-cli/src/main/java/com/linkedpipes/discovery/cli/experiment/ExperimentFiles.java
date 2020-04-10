package com.linkedpipes.discovery.cli.experiment;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.cli.pipeline.Pipeline;
import com.linkedpipes.discovery.cli.pipeline.PipelineCollector;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.statistics.Statistics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ExperimentFiles {

    private final List<CsvFile> csvFiles = new ArrayList<>();

    private final DiscoveryFile discoveryFile = new DiscoveryFile();

    public ExperimentFiles() {
        csvFiles.add(new ApplicationDiscoveryFile());
        csvFiles.add(new DatasetApplicationDiscoveryFile());
        csvFiles.add(new DiscoveryDatasetFile());
        csvFiles.add(discoveryFile);
    }

    public void add(
            String path, Discovery discovery, Dataset dataset,
            Statistics statistics) {
        PipelineCollector pipelineCollector = new PipelineCollector();
        List<Pipeline> pipelines = pipelineCollector.collect(
                dataset, discovery.getRoot());
        for (CsvFile csvFile : csvFiles) {
            csvFile.add(path, discovery, dataset, statistics, pipelines);
        }
    }

    public void write(
            File directory, Map<String, Long> durationsInSeconds)
            throws IOException {
        discoveryFile.addDiscoveryDurations(durationsInSeconds);
        for (CsvFile csfFile : csvFiles) {
            csfFile.write(directory);
        }
    }

}
