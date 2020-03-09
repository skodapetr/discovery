package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.filter.DiffBasedFilter;
import com.linkedpipes.discovery.filter.DiffBasedFilterForDiffStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Can be used only with {@link DiffBasedFilterForDiffStore}.
 * Do not use with {@link DiffBasedFilter} as that would lead to duplication
 * of the DIFF leading to a very bad performance.
 */
public class DiffStore implements SampleStore {

    public static class Diff {

        public final List<Statement> added;

        public final List<Statement> removed;

        protected Diff(List<Statement> added, List<Statement> removed) {
            this.added = added;
            this.removed = removed;
        }

        public Set<Statement> toSet() {
            Set<Statement> result = new HashSet<>();
            result.addAll(added);
            result.addAll(removed);
            return result;
        }

        public List<Statement> toList() {
            List<Statement> result = new ArrayList<>(
                    added.size() + removed.size());
            result.addAll(added);
            result.addAll(removed);
            return result;
        }

        public Integer size() {
            return added.size() + removed.size();
        }

        public static Diff fromDiff(
                List<Statement> added, List<Statement> removed) {
            return new Diff(added, removed);
        }

        public static Diff create(
                Set<Statement> root,
                Collection<Statement> statements) {
            Set<Statement> nodeSample = new HashSet<>(statements);
            // Newly added compare to root - those not in root.
            List<Statement> added = nodeSample.stream()
                    .filter(st -> !root.contains(st))
                    .collect(Collectors.toList());
            // Removed compare to root - those from root not in sample.
            List<Statement> removed = root.stream()
                    .filter(st -> !nodeSample.contains(st))
                    .collect(Collectors.toList());
            return new Diff(added, removed);
        }

    }

    private Set<Statement> root;

    private final boolean keepInMemory;

    private final Map<SampleRef, Diff> diffMap = new HashMap<>();

    private final Timer createDiffTimer;

    private final Timer constructDiffTimer;

    public DiffStore(boolean keepInMemory, MeterRegistry registry) {
        this.keepInMemory = keepInMemory;
        this.createDiffTimer =
                registry.timer(MeterNames.FILTER_DIFF_CREATE);
        this.constructDiffTimer =
                registry.timer(MeterNames.DIFF_STORE_CONSTRUCT);
    }

    @Override
    public SampleRef storeRoot(List<Statement> statements) {
        SampleRef ref = new SampleRef("root");
        root = new HashSet<>(statements);
        Diff emptyDiff = Diff.create(
                Collections.emptySet(), Collections.emptyList());
        diffMap.put(ref, emptyDiff);
        return ref;
    }

    @Override
    public SampleRef store(List<Statement> statements, String name) {
        SampleRef ref = new SampleRef(name);
        store(statements, ref);
        return ref;
    }

    @Override
    public void store(List<Statement> statements, SampleRef ref) {
        Instant start = Instant.now();
        Diff diff = Diff.create(root, statements);
        createDiffTimer.record(Duration.between(start, Instant.now()));
        diffMap.put(ref, diff);
    }

    public SampleRef store(Diff diff) {
        SampleRef ref = new SampleRef(DiffBasedFilter.REF_NAME);
        diffMap.put(ref, diff);
        return ref;
    }

    @Override
    public List<Statement> load(SampleRef ref) {
        Diff diff = diffMap.get(ref);
        if (diff == null) {
            return null;
        }
        return construct(diff);
    }

    public List<Statement> construct(Diff diff) {
        Instant start = Instant.now();
        List<Statement> result = new ArrayList<>(
                root.size() + diff.added.size());
        result.addAll(root);
        result.removeAll(diff.removed);
        result.addAll(diff.added);
        constructDiffTimer.record(Duration.between(start, Instant.now()));
        return result;
    }

    @Override
    public void releaseFromMemory(SampleRef ref) {
        ref.memoryCount -= 1;
        if (keepInMemory) {
            return;
        }
        diffMap.remove(ref);
    }

    @Override
    public Iterator<Entry> iterate() {
        var iterator = diffMap.entrySet().iterator();
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Entry next() {
                var entry = iterator.next();
                return new Entry(entry.getKey(), construct(entry.getValue()));
            }

        };
    }

    @Override
    public void remove(SampleRef ref) {
        diffMap.remove(ref);
    }

    @Override
    public void cleanUp() {
        root = null;
        diffMap.clear();
    }

    public Diff getDiff(SampleRef ref) {
        return diffMap.get(ref);
    }

}
