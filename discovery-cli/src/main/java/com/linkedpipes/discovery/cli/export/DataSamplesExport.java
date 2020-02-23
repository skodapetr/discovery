package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.node.NodeFacade;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class DataSamplesExport {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataSamplesExport.class);

    public static void export(
            Node root, NodeToName nodeToName, NodeFacade nodeFacade,
            File directory) {
        directory.mkdirs();
        root.accept((node) -> {
            File output = new File(directory, nodeToName.name(node) + ".ttl");
            saveDataSample(node, nodeFacade, output);
        });
    }

    private static void saveDataSample(
            Node node, NodeFacade nodeFacade, File file) {
        try (var stream = new FileOutputStream(file)) {
            RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, stream);
            writer.startRDF();
            for (Statement statement : nodeFacade.getDataSample(node)) {
                writer.handleStatement(statement);
            }
            writer.endRDF();
        } catch (IOException | DiscoveryException ex) {
            LOG.warn("Can't write sample to: {}", file);
        }
    }

}
