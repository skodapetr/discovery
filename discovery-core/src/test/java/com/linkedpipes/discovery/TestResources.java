package com.linkedpipes.discovery;

import com.linkedpipes.discovery.rdf.RdfAdapter;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

public final class TestResources {

    public static List<Statement> asStatements(String fileName) {
        File file = file(fileName);
        try {
            return RdfAdapter.asStatements(file);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static File file(String fileName) {
        URL url = Thread.currentThread().getContextClassLoader()
                .getResource(fileName);
        if (url == null) {
            throw new RuntimeException("Required resource '"
                    + fileName + "' is missing.");
        }
        return new File(url.getPath());
    }

}
