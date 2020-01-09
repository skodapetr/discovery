package com.linkedpipes.discovery.rdf;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFHandler;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public final class RdfAdapter {

    public static List<Statement> asStatements(File file) throws IOException {
        RDFFormat format =
                Rio.getParserFormatForFileName(file.getName()).orElse(null);
        if (format == null) {
            throw new IOException(
                    "Can't determine file format : " + file.getName());
        }
        List<Statement> statements = new ArrayList<>();
        RDFParser rdfParser = Rio.createParser(format);
        rdfParser.setRDFHandler(new AbstractRDFHandler() {
            @Override
            public void handleStatement(Statement st) {
                statements.add(st);
            }
        });
        try (InputStream input = new FileInputStream(file)) {
            rdfParser.parse(input, "http://localhost/");
        }
        return statements;
    }

    public static List<Statement> asStatements(URL url) throws IOException {
        switch (url.getProtocol()) {
            case "file":
                try {
                    return asStatements(new File(url.toURI()));
                } catch (URISyntaxException ex) {
                    throw new IOException("Invalid URL.", ex);
                }
            case "http":
            case "https":
                return fromHttp(url);
            default:
                throw new IOException(
                        "Unsupported protocol: " + url.getProtocol());
        }
    }

    private static List<Statement> fromHttp(URL url) throws IOException {
        List<Statement> result = new ArrayList<>();
        try (InputStream stream = url.openStream()) {
            // TODO: This should use content-negotiation.
            RDFParser parser = Rio.createParser(RDFFormat.TURTLE);
            parser.setRDFHandler(new StatementCollector(result));
            parser.parse(stream, url.toString());
        }
        return result;
    }

}
