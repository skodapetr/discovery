package com.linkedpipes.discovery.cli.experiment;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.cli.pipeline.Pipeline;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.statistics.Statistics;
import com.opencsv.CSVWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

class DiscoveryFile extends CsvFile {

    private static class Line {

        public final String discovery;

        public int pipelineCount;

        /**
         * This property is set in {@link #addDiscoveryDurations}.
         */
        private long totalDurationInSeconds;

        private long discoveryDurationInSeconds;

        private final Set<String> applications = new HashSet<>();

        private final Set<String> usedInPipelineApplications = new HashSet<>();

        private final Set<String> datasets = new HashSet<>();

        private final Set<String> usedDatasets = new HashSet<>();

        private final Set<String> usedInPipelineDatasets = new HashSet<>();

        private final Set<String> transformers = new HashSet<>();

        private final Set<String> usedTransformers = new HashSet<>();

        private final Set<String> usedInPipelineTransformers = new HashSet<>();

        public Line(String discovery) {
            this.discovery = discovery;
        }

        public String[] asStringList() {
            return new String[]{
                    discovery,
                    Integer.toString(pipelineCount),
                    Long.toString(totalDurationInSeconds),
                    Long.toString(discoveryDurationInSeconds),
                    Integer.toString(applications.size()),
                    Integer.toString(usedInPipelineApplications.size()),
                    Integer.toString(datasets.size()),
                    Integer.toString(usedDatasets.size()),
                    Integer.toString(usedInPipelineDatasets.size()),
                    Integer.toString(transformers.size()),
                    Integer.toString(usedTransformers.size()),
                    Integer.toString(usedInPipelineTransformers.size())
            };
        }

    }

    private List<Line> lines = new ArrayList<>();

    @Override
    protected String getFileName() {
        return "discovery.csv";
    }

    @Override
    protected String[] getHeader() {
        return new String[]{
                "discovery",
                "pipelines count",
                "time (s) total",
                "time (s) discovery",
                "applications count",
                "used in pipelines applications count",
                "datasets counts",
                "used datasets counts",
                "used in pipelines datasets counts",
                "transformers count",
                "used transformers count",
                "used in pipelines transformers count",
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
        Line line = getLine(discovery);
        line.pipelineCount += pipelines.size();
        line.discoveryDurationInSeconds +=
                statistics.aggregate().durationInMilliSeconds / 1000;
        // All available.
        line.applications.addAll(appAsIri(discovery.getApplications()));
        line.datasets.add(dataset.iri);
        line.transformers.addAll(transAsIri(discovery.getTransformers()));
        // Used, something (app, transformers) were applied.
        Statistics.Level agg = statistics.aggregate();
        line.usedTransformers.addAll(transAsIri(agg.transformers));
        if (!agg.transformers.isEmpty() || !agg.applications.isEmpty()) {
            line.usedDatasets.add(dataset.iri);
        }
        // Used in a pipeline.
        line.usedInPipelineApplications.addAll(appAsIri(agg.applications));
        if (!pipelines.isEmpty()) {
            line.usedInPipelineDatasets.add(dataset.iri);
        }
        line.usedInPipelineTransformers.addAll(
                transAsIri(collectUsedInPipelineTransformers(pipelines)));
    }

    private Line getLine(Discovery discovery) {
        for (Line line : lines) {
            if (line.discovery.equals(discovery.getIri())) {
                return line;
            }
        }
        Line newLine = new Line(discovery.getIri());
        lines.add(newLine);
        return newLine;
    }

    private Set<String> appAsIri(Collection<Application> applications) {
        return applications
                .stream()
                .map(application -> application.iri)
                .collect(Collectors.toSet());
    }

    private Set<String> transAsIri(Collection<Transformer> transformers) {
        return transformers
                .stream()
                .map(transformer -> transformer.iri)
                .collect(Collectors.toSet());
    }

    private Set<Transformer> collectUsedInPipelineTransformers(
            List<Pipeline> pipelines) {
        Set<Transformer> result = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            result.addAll(pipeline.transformers);
        }
        return result;
    }

    public void addDiscoveryDurations(Map<String, Long> durationInSeconds) {
        for (Line line : lines) {
            long duration = durationInSeconds.getOrDefault(line.discovery, 0L);
            line.totalDurationInSeconds += duration;
        }
    }

}

