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

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

public class FromExperiment extends DiscoveryBuilder {

    private static final IRI HAS_TEMPLATE;

    private static final IRI HAS_DATA_SAMPLE;

    static {
        SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();
        HAS_TEMPLATE = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/"
                        + "discovery/hasTemplate");
        HAS_DATA_SAMPLE = valueFactory.createIRI(
                "https://discovery.linkedpipes.com/vocabulary/"
                        + "outputDataSample");
    }

    private final String url;

    private Dataset dataset = null;

    public FromExperiment(String url) {
        this.url = url;
    }

    @Override
    public Discovery create(MeterRegistry registry) throws Exception {
        List<String> templates = loadTemplateUrls();
        loadTemplates(templates);
        checkIsValid();
        FilterStrategy filterStrategy = getFilterStrategy(registry);
        return new Discovery(
                dataset, transformers, applications, filterStrategy, registry);
    }

    private List<String> loadTemplateUrls() throws IOException {
        List<Statement> statements = RdfAdapter.asStatements(new URL(url));
        // We do not filter by URL as the URL may contain
        // extension.
        return statements.stream()
                .filter(st -> st.getPredicate().equals(HAS_TEMPLATE))
                .filter(st -> st.getObject() instanceof IRI)
                .map(st -> st.getObject().stringValue())
                .collect(Collectors.toList());
    }

    private void loadTemplates(List<String> templates)
            throws Exception {
        for (String url : templates) {
            List<Statement> statements = RdfAdapter.asStatements(new URL(url));
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
            throw new Exception("Missing dataset");
        }
    }

}
