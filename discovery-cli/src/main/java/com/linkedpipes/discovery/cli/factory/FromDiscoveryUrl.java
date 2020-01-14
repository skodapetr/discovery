package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.filter.FilterStrategy;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FromDiscoveryUrl extends DiscoveryBuilder {

    private static final Logger LOG =
            LoggerFactory.getLogger(FromDiscoveryUrl.class);

    private static final IRI HAS_TEMPLATE;

    private static final IRI HAS_DATA_SAMPLE;

    private static final IRI HAS_IMPORT;

    static {
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        HAS_TEMPLATE = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/"
                        + "discovery/hasTemplate");
        HAS_DATA_SAMPLE = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/"
                        + "outputDataSample");
        HAS_IMPORT = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/" +
                        "discovery/import");
    }

    private final String discoveryUrl;

    private Dataset dataset = null;

    private boolean ignoreIssues = false;

    private File report = null;

    public FromDiscoveryUrl(String url) {
        this.discoveryUrl = url;
    }

    @Override
    public Discovery create(MeterRegistry registry) throws Exception {
        List<String> templates = loadTemplateUrls(discoveryUrl);
        loadTemplates(templates);
        checkIsValid();
        FilterStrategy filterStrategy = getFilterStrategy(registry);
        return new Discovery(
                dataset, transformers, applications, filterStrategy, registry);
    }

    private List<String> loadTemplateUrls(String url) throws IOException {
        List<Statement> statements;
        try {
            statements = RdfAdapter.asStatements(new URL(url));
        } catch (IOException ex) {
            if (!ignoreIssues) {
                throw ex;
            }
            LOG.warn("Can't resolve URL: {}", url);
            appendToReport("Invalid IMPORT url: " + url);
            return new ArrayList<>();
        }
        // We do not filter by URL as the URL may contain
        // extension.
        List<String> result = statements.stream()
                .filter(st -> st.getPredicate().equals(HAS_TEMPLATE))
                .filter(st -> st.getObject() instanceof IRI)
                .map(st -> st.getObject().stringValue())
                .collect(Collectors.toList());
        for (Statement statement : statements) {
            if (!statement.getPredicate().equals(HAS_IMPORT)) {
                continue;
            }
            if (!(statement.getSubject() instanceof IRI)) {
                continue;
            }
            result.addAll(loadTemplateUrls(
                    statement.getSubject().stringValue()));
        }
        return result;
    }

    private void loadTemplates(List<String> templates)
            throws Exception {
        for (String url : templates) {
            List<Statement> statements;
            try {
                statements = RdfAdapter.asStatements(new URL(url));
            } catch (Exception ex) {
                if (!ignoreIssues) {
                    throw ex;
                }
                LOG.warn("Can't resolve template: {}", url, ex);
                appendToReport("Invalid template URL: " + url);
                continue;
            }
            for (Statement statement : statements) {
                if (!statement.getSubject().stringValue().equals(url)) {
                    continue;
                }
                if (!statement.getPredicate().equals(RDF.TYPE)) {
                    continue;
                }
                switch (statement.getObject().stringValue()) {
                    case Dataset.TYPE:
                        onDataset(url, statements);
                        break;
                    case Application.TYPE:
                        onApplication(statements);
                        break;
                    case Transformer.TYPE:
                        onTransformer(statements);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void onDataset(String iri, List<Statement> statements)
            throws Exception {
        for (Statement statement : statements) {
            if (!statement.getPredicate().equals(HAS_DATA_SAMPLE)) {
                continue;
            }
            String sampleUrl = statement.getObject().stringValue();
            List<Statement> dataSample =
                    RdfAdapter.asStatements(new URL(sampleUrl));
            dataset = ModelAdapter.loadDataset(iri, dataSample);
            return;
        }
        throw new Exception("Missing data sample for dataset.");
    }

    private void onApplication(List<Statement> statements)
            throws UnexpectedInput {
        applications.add(ModelAdapter.loadApplication(statements));
    }

    private void onTransformer(List<Statement> statements)
            throws UnexpectedInput {
        transformers.add(ModelAdapter.loadTransformer(statements));
    }

    private void checkIsValid() throws Exception {
        if (dataset == null) {
            appendToReport("Missing dataset.");
            throw new Exception("Missing dataset");
        }
    }

    private void appendToReport(String line) throws IOException {
        if (report == null) {
            return;
        }
        report.getParentFile().mkdirs();
        try (var writer = new PrintWriter(
                new OutputStreamWriter(
                        new FileOutputStream(report, true),
                        StandardCharsets.UTF_8))) {
            writer.write(line);
            writer.write("\n");
        }
    }

    public void setIgnoreIssues(boolean ignoreIssues) {
        this.ignoreIssues = ignoreIssues;
    }

    public void setReport(File report) {
        this.report = report;
    }

}
