package com.linkedpipes.discovery.cli.experiment;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.cli.pipeline.Pipeline;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.statistics.Statistics;
import com.opencsv.CSVWriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class ApplicationDiscoveryFile extends CsvFile {

    private static class Line {

        private final String application;

        private final String discovery;

        private final String applicationLabel;

        private Set<String> datasets = new HashSet<>();

        private Set<String> usedInPipelineDatasets = new HashSet<>();

        private int pipelinesCount;

        public Line(Application application, Discovery discovery) {
            this.application = application.iri;
            this.discovery = discovery.getIri();
            this.applicationLabel = application.title.asString();
        }

        public String[] asStringList() {
            return new String[]{
                    application,
                    discovery,
                    applicationLabel,
                    Integer.toString(usedInPipelineDatasets.size()),
                    Integer.toString(pipelinesCount)
            };
        }

    }

    @Override
    protected String getFileName() {
        return "application-discovery.csv";
    }

    @Override
    protected String[] getHeader() {
        return new String[]{
                "application",
                "discovery",
                "application label",
                "dataset count",
                "used in pipelines dataset count",
                "pipelines count",
        };
    }

    private List<Line> lines = new ArrayList<>();

    @Override
    protected void writeLines(CSVWriter writer) {
        lines.forEach((line) -> writer.writeNext(line.asStringList()));
    }

    @Override
    public void add(
            String path, Discovery discovery, Dataset dataset,
            Statistics statistics, List<Pipeline> pipelines) {
        Statistics.Level agg = statistics.aggregate();
        for (Application application : agg.applications) {
            Line line = getLine(discovery, application);
            List<Pipeline> appPipelines =
                    selectForApplication(pipelines, application);
            line.pipelinesCount += appPipelines.size();
            // Used in pipelines.
            if (!appPipelines.isEmpty()) {
                line.usedInPipelineDatasets.add(dataset.iri);
            }
        }
        // For all applications add dataset.
        for (Application application : discovery.getApplications()) {
            Line line = getLine(discovery, application);
            line.datasets.add(dataset.iri);
        }
    }

    private Line getLine(Discovery discovery, Application application) {
        for (Line line : lines) {
            if (line.discovery.equals(discovery.getIri())
                    && line.application.equals(application.iri)) {
                return line;
            }
        }
        Line newLine = new Line(application, discovery);
        lines.add(newLine);
        return newLine;
    }

    private List<Pipeline> selectForApplication(
            List<Pipeline> pipelines, Application application) {
        return pipelines
                .stream()
                .filter(pipeline ->
                        pipeline.application.iri.equals(application.iri))
                .collect(Collectors.toList());
    }

}
