package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.cli.export.SummaryExport;
import com.linkedpipes.discovery.cli.factory.DiscoveryBuilder;
import com.linkedpipes.discovery.cli.factory.FromDiscoveryUrl;
import com.linkedpipes.discovery.cli.model.DiscoveryStatisticsInPath;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for running experiments ~ collections of discoveries.
 */
public class ExperimentRunner {

    private static final Logger LOG =
            LoggerFactory.getLogger(ExperimentRunner.class);

    private static final IRI HAS_DISCOVERY;

    static {
        SimpleValueFactory factory = SimpleValueFactory.getInstance();
        HAS_DISCOVERY = factory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/experiment/"
                        + "hasDiscovery");
    }

    private final boolean ignoreIssues;

    private final int iterationLimit;

    public ExperimentRunner(boolean ignoreIssues, int iterationLimit) {
        this.ignoreIssues = ignoreIssues;
        this.iterationLimit = iterationLimit;
    }

    public void run(String url, File outputRoot) throws Exception {
        Instant start = Instant.now();
        List<String> discoveries = getDiscoveries(url);
        LOG.info("Collected {} discoveries in experiment {}",
                discoveries.size(), url);
        List<DiscoveryStatisticsInPath> result = new ArrayList<>();
        MeterRegistry registry = AppEntry.createMeterRegistry();
        for (int index = 0; index < discoveries.size(); ++index) {
            String name = String.format("%03d", index);
            File output = new File(outputRoot, name);
            File working = new File(output, "working");
            DiscoveryBuilder builder = prepareDiscoveryBuilder(
                    discoveries.get(index), output);
            var stats = AppEntry.runDiscovery(
                    builder, iterationLimit, registry, output, working);
            // Modify path to reflect location in a subdirectory.
            stats.forEach(item -> item.path = name + "/" + item.path);
            result.addAll(stats);
        }
        AppEntry.logMeterRegistry(registry);
        SummaryExport.export(result, outputRoot);
        LOG.info("All done in: {} min",
                Duration.between(start, Instant.now()).toMinutes());
    }

    @SuppressFBWarnings(value = {"DM_EXIT"})
    private List<String> getDiscoveries(String url) {
        List<Statement> statements;
        try {
            statements = RdfAdapter.asStatements(new URL(url));
        } catch (IOException ex) {
            LOG.warn("Can't resolve experiment URL: {}", url);
            System.exit(1);
            // IDEA fail to detect above as application end.
            throw new RuntimeException();
        }
        List<String> result = new ArrayList<>();
        for (Statement statement : statements) {
            if (!statement.getPredicate().equals(HAS_DISCOVERY)) {
                continue;
            }
            if (!(statement.getObject() instanceof Resource)) {
                continue;
            }
            Resource object = (Resource) statement.getObject();
            if (isListHead(statements, object)) {
                result.addAll(loadUrlList(statements, object));
            } else {
                result.add(object.stringValue());
            }
        }
        return result;
    }

    private static boolean isListHead(
            List<Statement> statements, Resource resource) {
        for (Statement statement : statements) {
            if (!statement.getSubject().equals(resource)) {
                continue;
            }
            if (RDF.FIRST.equals(statement.getPredicate())) {
                return true;
            }
        }
        return false;
    }

    private static List<String> loadUrlList(
            List<Statement> statements, Resource resource) {
        List<String> result = new ArrayList<>();
        while (resource != null) {
            Resource rest = null;
            for (Statement statement : statements) {
                if (!statement.getSubject().equals(resource)) {
                    continue;
                }
                if (RDF.FIRST.equals(statement.getPredicate())) {
                    if (statement.getObject() instanceof Resource) {
                        result.add(statement.getObject().stringValue());
                    }
                }
                if (RDF.REST.equals(statement.getPredicate())) {
                    if (statement.getObject() instanceof Resource) {
                        rest = (Resource) statement.getObject();
                    }
                }
            }
            resource = rest;
        }
        return result;
    }

    private DiscoveryBuilder prepareDiscoveryBuilder(String url, File output) {
        FromDiscoveryUrl builder = new FromDiscoveryUrl(url);
        if (ignoreIssues) {
            builder.setIgnoreIssues(true);
            builder.setReport(new File(output, "builder-report.txt"));
        }
        return builder;
    }

}
