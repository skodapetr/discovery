package com.linkedpipes.discovery.cli.export;

import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.store.SampleStore;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

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
        if (!node.isExpanded() || node.isRedundant()) {
            return;
        }
        List<Statement> dataSample;
        try {
            dataSample = sampleStore.load(node.getDataSampleRef());
        } catch (Exception ex) {
            LOG.warn("Error getting data sample for: {} to: {}",
                    node.getDataSampleRef(),
                    file.getName());
            return;
        }
        if (dataSample == null) {
            LOG.warn("Missing data (null) sample for: {} to: {}",
                    node.getDataSampleRef(),
                    file.getName());
            return;
        }
        try (var stream = new FileOutputStream(file)) {
            RDFWriter writer = Rio.createWriter(RDFFormat.TURTLE, stream);
            writer.startRDF();
            for (var statement : dataSample) {
                writer.handleStatement(statement);
            }
            writer.endRDF();
        } catch (IOException ex) {
            LOG.warn("Can't write sample to: {}", file);
        }
    }

}
