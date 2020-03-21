package com.linkedpipes.discovery.sample;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.util.Models;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DataSampleDiff {

    public final List<Statement> added;

    public final List<Statement> removed;

    protected DataSampleDiff(List<Statement> added, List<Statement> removed) {
        this.added = added;
        this.removed = removed;
    }

    public Integer size() {
        return added.size() + removed.size();
    }

    /**
     * Symmetric operation.
     */
    public boolean match(DataSampleDiff right) {
        return added.size() == right.added.size()
                && Models.isomorphic(added, right.added)
                && Models.isomorphic(removed, right.removed);
    }
    
    public static DataSampleDiff fromDiff(
            List<Statement> added, List<Statement> removed) {
        return new DataSampleDiff(added, removed);
    }

    public static DataSampleDiff create(
            Set<Statement> root, Collection<Statement> statements) {
        Set<Statement> nodeSample = new HashSet<>(statements);
        // Newly added compare to root - those not in root.
        List<Statement> added = nodeSample.stream()
                .filter(st -> !root.contains(st))
                .collect(Collectors.toList());
        // Removed compare to root - those from root not in sample.
        List<Statement> removed = root.stream()
                .filter(st -> !nodeSample.contains(st))
                .collect(Collectors.toList());
        return new DataSampleDiff(added, removed);
    }


}
