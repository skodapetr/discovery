package com.linkedpipes.discovery.cli.experiment;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.cli.pipeline.Pipeline;
import com.linkedpipes.discovery.cli.pipeline.PipelineCollector;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.statistics.Statistics;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ExperimentFiles {

    private final List<CsvFile> csvFiles = Arrays.asList(
            new ApplicationDiscoveryFile(),
            new DatasetApplicationDiscoveryFile(),
            new DiscoveryDatasetFile(),
            new DiscoveryFile()
    );

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

    public void write(File directory) throws IOException {
        for (CsvFile csfFile : csvFiles) {
            csfFile.write(directory);
        }
    }

}
