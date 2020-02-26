package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

class FileStorage implements SampleStore {

    private final Map<SampleRef, File> fileStorage = new HashMap<>();

    private final File directory;

    private final Timer fileSystemTimer;

    /**
     * The working directory is not owned by FileStorage and so it must
     * be deleted by the owner.
     */
    public FileStorage(File directory, MeterRegistry registry) {
        this.directory = directory;
        this.fileSystemTimer = registry.timer(MeterNames.DATA_SAMPLE_STORAGE);
        this.directory.mkdirs();
    }

    @Override
    public SampleRef store(List<Statement> statements, String name)
            throws DiscoveryException {
        SampleRef ref = new SampleRef();
        Instant start = Instant.now();
        try {
            File file = Files.createTempFile(
                    directory.toPath(), name + "-", ".n3").toFile();
            RdfAdapter.toFile(statements, file);
            fileStorage.put(ref, file);
        } catch (IOException ex) {
            throw new DiscoveryException(
                    "Can't save data sample to file.", ex);
        } finally {
            fileSystemTimer.record(Duration.between(start, Instant.now()));
        }
        return ref;
    }

    @Override
    public List<Statement> load(SampleRef ref) throws DiscoveryException {
        File file = fileStorage.get(ref);
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

    @Override
    public void cleanUp() {
        // We not not delete the working directory.
        fileStorage.clear();
    }

}
