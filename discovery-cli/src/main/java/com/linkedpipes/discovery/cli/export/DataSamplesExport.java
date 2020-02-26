package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.SampleStore;
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
            Node root, NodeToName nodeToName, SampleStore sampleStore,
            File directory) {
        directory.mkdirs();
        root.accept((node) -> {
            File output = new File(directory, nodeToName.name(node) + ".ttl");
            saveDataSample(node, sampleStore, output);
        });
    }

    private static void saveDataSample(
            Node node, SampleStore sampleStore, File file) {
        try (var stream = new FileOutputStream(file)) {
            RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, stream);
            writer.startRDF();
            for (var statement : sampleStore.load(node.getDataSampleRef())) {
                writer.handleStatement(statement);
            }
            writer.endRDF();
        } catch (IOException | DiscoveryException ex) {
            LOG.warn("Can't write sample to: {}", file);
        }
    }

}
