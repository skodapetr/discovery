package com.linkedpipes.discovery.cli;

import com.linkedpipes.discovery.SuppressFBWarnings;
import com.linkedpipes.discovery.cli.export.SummaryExport;
import com.linkedpipes.discovery.cli.factory.BuilderConfiguration;
import com.linkedpipes.discovery.cli.model.NamedDiscoveryStatistics;
import com.linkedpipes.discovery.rdf.RdfAdapter;
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
public class RunExperiment {

    private static final Logger LOG =
            LoggerFactory.getLogger(RunExperiment.class);

    private static final IRI HAS_DISCOVERY;

    static {
        SimpleValueFactory factory = SimpleValueFactory.getInstance();
        HAS_DISCOVERY = factory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/experiment/"
                        + "hasDiscovery");
    }

    private final BuilderConfiguration configuration;

    public RunExperiment(BuilderConfiguration configuration) {
        this.configuration = configuration;
    }

    public void run(String experiment) throws Exception {
        Instant start = Instant.now();
        List<String> discoveries = getDiscoveries(experiment);
        LOG.info("Collected {} discoveries in experiment {}",
                discoveries.size(), experiment);
        List<NamedDiscoveryStatistics> result = new ArrayList<>();
        for (int index = 0; index < discoveries.size(); ++index) {
            String name = String.format("%03d", index);
            BuilderConfiguration discoveryConfig = configuration.copy();
            discoveryConfig.output = new File(configuration.output, name);
            RunDiscovery runner = new RunDiscovery(discoveryConfig);
            var stats = runner.run(discoveries.get(index));
            // Modify path to reflect location in a subdirectory.
            // We also replace discovery IRI, instead of {iri}/{???},
            // we set it to {iri}.
            // The reason for this is, that old discovery runs a single
            // discovery for multiple data sources. But we ru a discovery
            // only for a single data source. We to address that we split
            // a single discovery into more discoveries using the {???} suffix.
            for (NamedDiscoveryStatistics statistics : stats) {
                statistics.name = name + "/" + statistics.name;
                statistics.discoveryIri = discoveries.get(index);
            }
            result.addAll(stats);
        }
        SummaryExport.export(result, configuration.output);
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

    private boolean isListHead(
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

    private List<String> loadUrlList(
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

}
