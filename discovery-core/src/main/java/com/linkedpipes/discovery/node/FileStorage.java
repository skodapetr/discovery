package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.apache.commons.io.FileUtils;
import org.eclipse.rdf4j.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FileStorage implements DataSampleStorage {

    /**
     * When memory utilization (used / max) is bigger then this threshold,
     * new data samples are saved file system.
     */
    private static final double MEMORY_UTILIZATION_FOR_IN_MEMORY_STORE = 0.8;

    private static final Logger LOG =
            LoggerFactory.getLogger(DataSampleStorage.class);

    private final Map<Node, List<Statement>> memoryStore = new HashMap<>();

    private final Map<Node, File> nodeToFile = new HashMap<>();

    private final File directory;

    private final Runtime runtime = Runtime.getRuntime();

    private final Timer fileSystemTimer;

    private boolean fileSystemUsed = false;

    public FileStorage(File directory, MeterRegistry registry) {
        this.directory = directory;
        this.fileSystemTimer =
                registry.timer(MeterNames.DATA_SAMPLE_STORAGE);
    }

    public void setDataSample(Node node, List<Statement> dataSample)
            throws DiscoveryException {
        if (memoryStore.containsKey(node)) {
            memoryStore.put(node, dataSample);
            return;
        }
        if (shouldSaveInMemory()) {
            memoryStore.put(node, dataSample);
        } else {
            saveToFile(node, dataSample);
        }
    }

    protected boolean shouldSaveInMemory() {
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        double share = (double) usedMemory / maxMemory;
        return share < MEMORY_UTILIZATION_FOR_IN_MEMORY_STORE;
    }

    protected void saveToFile(Node node, List<Statement> dataSample)
            throws DiscoveryException {
        if (!fileSystemUsed) {
            fileSystemUsed = true;
            LOG.info("Using file system to store data samples.");
        }

        Instant start = Instant.now();
        try {
            File file = Files.createTempFile(
                    directory.toPath(), "data-sample", ".n3").toFile();
            RdfAdapter.toFile(dataSample, file);
            nodeToFile.put(node, file);
        } catch (IOException ex) {
            throw new DiscoveryException(
                    "Can't save data sample to file.", ex);
        } finally {
            fileSystemTimer.record(Duration.between(start, Instant.now()));
        }
    }

    public List<Statement> getDataSample(Node node)
            throws DiscoveryException {
        if (memoryStore.containsKey(node)) {
            return Collections.unmodifiableList(memoryStore.get(node));
        }
        if (nodeToFile.containsKey(node)) {
            return loadDataSample(node);
        }
        throw new DiscoveryException("No data found for given node.");
    }

    private List<Statement> loadDataSample(Node node)
            throws DiscoveryException {
        File file = nodeToFile.get(node);
        Instant start = Instant.now();
        try {
            return RdfAdapter.asStatements(file);
        } catch (IOException ex) {
            throw new DiscoveryException(
                    "Can't load node sample data from {}", file, ex);
        } finally {
            fileSystemTimer.record(Duration.between(start, Instant.now()));
        }
    }

    public void deleteDataSample(Node node) {
        memoryStore.remove(node);
        nodeToFile.remove(node);
        // We leave the file to be deleted later.
    }

    public void cleanUp() {
        memoryStore.clear();
        nodeToFile.clear();
        try {
            FileUtils.deleteDirectory(directory);
        } catch (IOException ex) {
            LOG.info("Can't delete sample storage directory.");
        }
    }

}
