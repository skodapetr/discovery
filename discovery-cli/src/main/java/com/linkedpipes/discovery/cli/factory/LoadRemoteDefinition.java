package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.model.TransformerGroup;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
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

class LoadRemoteDefinition {

    private static final Logger LOG =
            LoggerFactory.getLogger(LoadRemoteDefinition.class);

    private static final IRI HAS_TEMPLATE;

    private static final IRI HAS_DATA_SAMPLE;

    private static final IRI HAS_IMPORT;

    private static final IRI HAS_TRANSFORMER_GROUP;

    private static final IRI INPUT;

    static {
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        HAS_TEMPLATE = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/"
                        + "discovery/hasTemplate");
        HAS_DATA_SAMPLE = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/"
                        + "outputDataSample");
        HAS_IMPORT = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/"
                        + "discovery/import");
        HAS_TRANSFORMER_GROUP = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/discovery/"
                        + "hasTransformerGroup");
        INPUT = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/discovery/Input");
    }

    protected BuilderConfiguration configuration;

    private final String discoveryUrl;

    private List<Dataset> datasets = new ArrayList<>();

    private List<Application> applications = new ArrayList<>();

    private List<Transformer> transformers = new ArrayList<>();

    private List<TransformerGroup> groups = new ArrayList<>();

    public LoadRemoteDefinition(
            BuilderConfiguration configuration, String discoveryUrl) {
        this.configuration = configuration;
        this.discoveryUrl = discoveryUrl;
    }

    public void load() throws Exception {
        LOG.info("Collecting templates for: {}", discoveryUrl);
        List<String> templates = loadTemplateUrls(discoveryUrl);
        LOG.info("Loading templates ...");
        loadTemplates(templates);
        LOG.info("Loaded applications: {} transformers: {} datasets: {}",
                applications.size(),
                transformers.size(),
                datasets.size());
        checkIsValid();
    }

    private List<String> loadTemplateUrls(String url) throws IOException {
        List<Statement> statements;
        try {
            statements = RdfAdapter.asStatements(new URL(url));
        } catch (IOException ex) {
            if (!configuration.ignoreIssues) {
                throw ex;
            }
            LOG.warn("Can't resolve URL: {}", url);
            appendToReport("Invalid import url: " + url);
            return new ArrayList<>();
        }
        // We do not filter by URL as the URL may contain
        // extension.
        List<Resource> inputs = statements.stream()
                .filter(st -> RDF.TYPE.equals(st.getPredicate()))
                .filter(st -> st.getObject().equals(INPUT))
                .map(st -> st.getSubject())
                .collect(Collectors.toList());
        List<String> templates = new ArrayList<>();
        if (inputs.size() != 1) {
            LOG.warn("Invalid number of inputs ({}) for URL: {}",
                    inputs.size(), url);
            appendToReport("Invalid number of inputs for: " + url);
            return new ArrayList<>();
        }
        Resource input = inputs.get(0);
        for (Statement statement : statements) {
            if (!statement.getSubject().equals(input)) {
                continue;
            }
            if (HAS_IMPORT.equals(statement.getPredicate())) {
                if (statement.getObject() instanceof IRI) {
                    templates.addAll(loadTemplateUrls(
                            statement.getObject().stringValue()));
                }
            } else if (HAS_TEMPLATE.equals(statement.getPredicate())) {
                if (statement.getObject() instanceof IRI) {
                    templates.add(statement.getObject().stringValue());
                }
            } else if (HAS_TRANSFORMER_GROUP.equals(statement.getPredicate())) {
                if (statement.getObject() instanceof Resource) {
                    onTransformerGroup(
                            (Resource)statement.getObject(), statements);
                }
            }
        }
        return templates;
    }

    private void appendToReport(String line) throws IOException {
        File report = configuration.reportFile();
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

    private void loadTemplates(List<String> templates)
            throws Exception {
        for (String url : templates) {
            List<Statement> statements;
            try {
                statements = RdfAdapter.asStatements(new URL(url));
            } catch (Exception ex) {
                if (!configuration.ignoreIssues) {
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
                        onDataset(statement.getSubject(), statements);
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

    private void onDataset(Resource iri, List<Statement> statements)
            throws Exception {
        for (Statement statement : statements) {
            if (!statement.getPredicate().equals(HAS_DATA_SAMPLE)) {
                continue;
            }
            String sampleUrl = statement.getObject().stringValue();
            List<Statement> dataSample;
            try {
                dataSample = RdfAdapter.asStatements(new URL(sampleUrl));
            } catch (Exception ex) {
                if (!configuration.ignoreIssues) {
                    throw ex;
                }
                LOG.warn("Can't resolve dataset sample on {} for {}",
                        sampleUrl, iri.stringValue(), ex);
                appendToReport("Invalid dataset sample URL: " + sampleUrl);
                return;
            }
            datasets.add(ModelAdapter.loadDataset(iri, statements, dataSample));
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

    private void onTransformerGroup(
            Resource resource, List<Statement> statements) {
        groups.add(ModelAdapter.loadTransformerGroup(statements, resource));
    }

    private void checkIsValid() throws Exception {
        if (datasets.isEmpty()) {
            appendToReport("Missing dataset.");
            throw new Exception("Missing dataset");
        }
    }

    public List<Dataset> getDatasets() {
        return datasets;
    }

    public List<Application> getApplications() {
        return applications;
    }

    public List<Transformer> getTransformers() {
        return transformers;
    }

    public List<TransformerGroup> getGroups() {
        return groups;
    }

}