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

class DiscoveryDatasetFile extends CsvFile {

    private static class Line {

        public String path;

        public String dataset;

        public String sparqlEndpoint;

        public String discovery;

        public String datasetLabel;

        public int pipelinesCount;

        public int applicationsCount;

        public String[] asStringList() {
            return new String[]{
                    path, dataset, sparqlEndpoint, discovery, datasetLabel,
                    Integer.toString(pipelinesCount),
                    Integer.toString(applicationsCount)
            };
        }

    }

    private List<Line> lines = new ArrayList<>();

    @Override
    protected String getFileName() {
        return "discovery-dataset.csv";
    }

    @Override
    protected String[] getHeader() {
        return new String[]{
                "path",
                "dataset",
                "dataset sparql endpoint",
                "discovery",
                "dataset label",
                "pipelines count",
                "applications count",
        };
    }

    @Override
    protected void writeLines(CSVWriter writer) {
        lines.forEach((line) -> writer.writeNext(line.asStringList()));
    }

    @Override
    public void add(
            String path, Discovery discovery, Dataset dataset,
            Statistics statistics, List<Pipeline> pipelines) {
        Line line = new Line();
        lines.add(line);
        line.path = path;
        line.dataset = dataset.iri;
        line.sparqlEndpoint = dataset.getSparqlEndpointOr("");
        line.discovery = discovery.getIri();
        line.datasetLabel = dataset.title.asString();
        line.pipelinesCount = pipelines.size();
        line.applicationsCount = countApplications(pipelines);
    }

    private int countApplications(List<Pipeline> pipelines) {
        Set<Application> applications = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            applications.add(pipeline.application);
        }
        return applications.size();
    }

}
