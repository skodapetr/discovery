package com.linkedpipes.discovery.cli.experiment;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.cli.pipeline.Pipeline;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.statistics.Statistics;
import com.opencsv.CSVWriter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class DatasetApplicationDiscoveryFile extends CsvFile {

    private static class Line {

        public String path;

        public String dataset;

        public String sparqlEndpoint;

        public String application;

        public String discovery;

        public String datasetLabel;

        public String applicationLabel;

        public int pipelinesCount;

        public String[] asStringList() {
            return new String[]{
                    path,
                    dataset,
                    sparqlEndpoint,
                    application,
                    discovery,
                    datasetLabel,
                    applicationLabel,
                    Integer.toString(pipelinesCount)
            };
        }

    }

    private List<Line> lines = new ArrayList<>();

    @Override
    protected String getFileName() {
        return "dataset-application-discovery.csv";
    }

    @Override
    protected String[] getHeader() {
        return new String[]{
                "path",
                "dataset",
                "dataset sparql endpoint",
                "application",
                "discovery",
                "dataset label",
                "application label",
                "pipelines count"
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
        Statistics.Level agg = statistics.aggregate();
        for (Application application : agg.applications) {
            Line line = new Line();
            lines.add(line);
            line.path = path;
            line.dataset = dataset.iri;
            line.sparqlEndpoint = dataset.getSparqlEndpointOr("");
            line.application = application.iri;
            line.discovery = discovery.getIri();
            line.datasetLabel = dataset.title.asString();
            line.applicationLabel = application.title.asString();
            line.pipelinesCount +=
                    selectForApplication(pipelines, application).size();
        }
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
