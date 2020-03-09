package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The idea is to store objects, predicates and values separately.
 * This should be of a relatively small size as they are not changing much.
 *
 * <p>However, experiments shows that the memory utilization is similar to
 * MapMemoryStore. We just add time to construct/deconstruct statements.
 * The probable reason is, that the statements are already pointers and
 * so they are of similar size as the array. And as we share whole
 * statements it is actually more efficient.
 *
 * <p>But serializing statements may be slower then saving list of integers,
 * this may lead to better performance when used with secondary memory. This
 * solution experimentally proves to be 4 times slower compared to FileStorage.
 * But used a low amount of memory. We may thus revisit the in-memory version
 * of this model as the integer lists should not create the difference:
 * 2743 MB memory vs 240 MB disk storing 56.227 * 104 statements, i.e.
 * around 17.5 M integers + diffs.
 *
 * <p>As a final solution both memory/disk solutions are implemented. If
 * the directory is provided disk store is used, else memory store is used.
 *
 * <p>But as SimpleStatement is just three pointers (integers) the
 * MapStatements store actually do similar job to BreakupStore, just without
 * the Maps and ArrayLists. Thus it seems that this store provide
 * no value.
 */
class BreakupStore implements SampleStore {

    private static final Logger LOG =
            LoggerFactory.getLogger(BreakupStore.class);

    private Map<Resource, Integer> subjectsMap = new HashMap<>();

    private Map<IRI, Integer> predicatesMap = new HashMap<>();

    private Map<Value, Integer> objectsMap = new HashMap<>();

    private ArrayList<Resource> subjectsList = new ArrayList<>();

    private ArrayList<IRI> predicatesList = new ArrayList<>();

    private ArrayList<Value> objectsList = new ArrayList<>();

    private Map<SampleRef, File> fileStorage = new HashMap<>();

    private Map<SampleRef, ArrayList<Integer>> memoryStorage = new HashMap<>();

    private final ValueFactory factory = SimpleValueFactory.getInstance();

    private final Timer addTimer;

    private final Timer getTimer;

    private final Timer diskTimer;

    private final File directory;

    public BreakupStore(File directory, MeterRegistry registry) {
        this.addTimer = registry.timer(MeterNames.BREAKUP_STORE_ADD);
        this.getTimer = registry.timer(MeterNames.BREAKUP_STORE_GET);
        this.diskTimer = registry.timer(MeterNames.BREAKUP_STORE_IO);
        this.directory = directory;
        if (directory != null) {
            directory.mkdirs();
        }
    }

    @Override
    public SampleRef store(List<Statement> statements, String name)
            throws DiscoveryException {
        SampleRef ref = new SampleRef(name);
        store(statements, ref);
        return ref;
    }

    @Override
    public void store(List<Statement> statements, SampleRef ref)
            throws DiscoveryException {
        ArrayList<Integer> content = deconstruct(statements);
        save(content, ref);
    }

    private ArrayList<Integer> deconstruct(List<Statement> statements) {
        Instant start = Instant.now();
        ArrayList<Integer> result = new ArrayList<>(statements.size());
        for (Statement statement : statements) {
            result.add(add(
                    statement.getSubject(), subjectsMap, subjectsList));
            result.add(add(
                    statement.getPredicate(), predicatesMap, predicatesList));
            result.add(add(
                    statement.getObject(), objectsMap, objectsList));
        }
        addTimer.record(Duration.between(start, Instant.now()));
        return result;
    }

    private <T> Integer add(T value, Map<T, Integer> map, List<T> list) {
        if (map.containsKey(value)) {
            return map.get(value);
        } else {
            Integer index = list.size();
            map.put(value, index);
            list.add(value);
            return index;
        }
    }

    private void save(ArrayList<Integer> content, SampleRef ref)
            throws DiscoveryException {
        if (directory == null) {
            memoryStorage.put(ref, content);
        } else {
            saveToFile(content, ref);
        }
    }

    private void saveToFile(ArrayList<Integer> content, SampleRef ref)
            throws DiscoveryException {
        Instant start = Instant.now();
        try {
            File file = Files.createTempFile(
                    directory.toPath(), ref.name + "-", ".n3").toFile();
            try (var stream = new ObjectOutputStream(
                    new FileOutputStream(file))) {
                stream.writeObject(content);
            }
            fileStorage.put(ref, file);
        } catch (IOException ex) {
            throw new DiscoveryException(
                    "Can't save data sample to file.", ex);
        } finally {
            diskTimer.record(Duration.between(start, Instant.now()));
        }
    }

    @Override
    public List<Statement> load(SampleRef ref) throws DiscoveryException {
        ArrayList<Integer> content;
        if (directory == null) {
            content = memoryStorage.get(ref);
        } else {
            content = loadFromFile(ref);
        }
        return construct(content);
    }

    private ArrayList<Integer> loadFromFile(SampleRef ref)
            throws DiscoveryException {
        File file = fileStorage.get(ref);
        Instant start = Instant.now();
        try {
            try (var stream = new ObjectInputStream(
                    new FileInputStream(file))) {
                return (ArrayList<Integer>) stream.readObject();
            }
        } catch (IOException | ClassNotFoundException ex) {
            throw new DiscoveryException(
                    "Can't load node sample data from {}", file, ex);
        } finally {
            diskTimer.record(Duration.between(start, Instant.now()));
        }
    }

    public List<Statement> construct(List<Integer> statements) {
        Instant start = Instant.now();
        List<Statement> result = new ArrayList<>(statements.size() / 3);
        for (int index = 0; index < statements.size(); index += 3) {
            result.add(factory.createStatement(
                    subjectsList.get(statements.get(index)),
                    predicatesList.get(statements.get(index + 1)),
                    objectsList.get(statements.get(index + 2))));
        }
        getTimer.record(Duration.between(start, Instant.now()));
        return result;
    }

    @Override
    public void remove(SampleRef ref) {
        memoryStorage.remove(ref);
        fileStorage.remove(ref);
    }

    @Override
    public Iterator<Entry> iterate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanUp() {
        subjectsMap.clear();
        predicatesMap.clear();
        objectsMap.clear();
        subjectsList.clear();
        predicatesList.clear();
        objectsList.clear();
        fileStorage.clear();
    }

    @Override
    public void logAfterLevelFinished() {
        LOG.info("Breakup store subject: {} predicate: {} object: {} size: {}",
                subjectsList.size(), predicatesList.size(), objectsList.size(),
                fileStorage.size());
    }

}
