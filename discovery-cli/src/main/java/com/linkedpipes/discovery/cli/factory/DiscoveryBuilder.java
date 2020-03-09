package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.filter.DiffBasedFilter;
import com.linkedpipes.discovery.filter.DiffBasedFilterForDiffStore;
import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.filter.NoFilter;
import com.linkedpipes.discovery.filter.Rdf4jIsomorphic;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.DiffStore;
import com.linkedpipes.discovery.sample.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;

import java.io.File;
import java.util.List;

public abstract class DiscoveryBuilder {

    protected BuilderConfiguration configuration;

    public DiscoveryBuilder(BuilderConfiguration configuration) {
        this.configuration = configuration;
    }

    protected NodeFilter createFilterStrategy(
            SampleStore sampleStore, MeterRegistry meterRegistry) {
        switch (configuration.filter) {
            case "diff":
                if (sampleStore instanceof DiffStore) {
                    return new DiffBasedFilterForDiffStore(
                            (DiffStore) sampleStore, meterRegistry);
                } else {
                    return new DiffBasedFilter(sampleStore, meterRegistry);
                }
            case "isomorphic":
                return new Rdf4jIsomorphic(sampleStore, meterRegistry);
            case "no-filter":
                return new NoFilter();
            default:
                throw new RuntimeException("Invalid filter name");
        }
    }

    protected DataSampleTransformer createDataDataSampleTransformer(
            MeterRegistry meterRegistry) {
        if (configuration.useDataSampleMapping) {
            return DataSampleTransformer.mapStatements(meterRegistry);
        } else {
            return DataSampleTransformer.noAction();
        }
    }

    protected SampleStore createSampleStore(
            MeterRegistry meterRegistry, File directory) {
        switch (configuration.store) {
            case "memory":
                return SampleStore.memoryStore(true);
            case "diff":
                return SampleStore.diffStore(true, meterRegistry);
            case "disk":
                return SampleStore.fileSystemStore(
                        new File(directory, "/working/file-store"),
                        meterRegistry);
            case "memory-disk":
                return SampleStore.withCache(
                        0.80f,
                        (name) -> name.startsWith(DiffBasedFilter.REF_NAME)
                                || name.startsWith("root"),
                        SampleStore.memoryStore(false),
                        SampleStore.fileSystemStore(
                                new File(directory, "/working/file-store"),
                                meterRegistry));
            default:
                throw new RuntimeException(
                        "Unknown sample store: " + configuration.store);
        }
    }

    public abstract List<Discovery> create(MeterRegistry registry)
            throws Exception;

}
