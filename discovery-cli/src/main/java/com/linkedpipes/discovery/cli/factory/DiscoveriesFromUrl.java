package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryBuilder;
import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.filter.DiffBasedFilter;
import com.linkedpipes.discovery.filter.NoFilter;
import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.filter.Rdf4jIsomorphic;
import com.linkedpipes.discovery.listeners.PruneByStrongGroup;
import com.linkedpipes.discovery.listeners.ResourceStrategy;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.store.HierarchicalStore;
import com.linkedpipes.discovery.sample.store.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;

import java.io.File;
import java.util.Comparator;
import java.util.List;

public class DiscoveriesFromUrl {

    @FunctionalInterface
    public interface Handler {

        void handle(
                String name,
                File directory,
                Dataset dataset,
                Discovery discoveryContext) throws DiscoveryException;

    }

    protected BuilderConfiguration configuration;

    protected String discoveryUrl;

    public DiscoveriesFromUrl(
            BuilderConfiguration configuration, String discoveryUrl) {
        this.configuration = configuration;
        this.discoveryUrl = discoveryUrl;
    }

    public void create(MeterRegistry registry, Handler handler)
            throws Exception {
        LoadRemoteDefinition definition =
                new LoadRemoteDefinition(configuration, discoveryUrl);
        definition.load();
        List<Dataset> datasets = definition.getDatasets();
        // We force same ordering to allow use of resume.
        datasets.sort(Comparator.comparing(dataset -> dataset.iri));
        for (int index = 0; index < datasets.size(); ++index) {
            String name = "discovery_" + String.format("%03d", index);
            String iri = definition.getIri()
                    + "/" + String.format("%03d", index);
            File directory = new File(configuration.output, name);
            Discovery discovery = createDiscovery(
                    iri, directory, datasets.get(index), definition, registry);
            addResourceStrategy(discovery);
            addOptionalListeners(definition, discovery);
            handler.handle(name, directory, datasets.get(index), discovery);
        }
    }

    private Discovery createDiscovery(
            String iri, File directory,
            Dataset dataset, LoadRemoteDefinition definition,
            MeterRegistry registry) throws DiscoveryException {
        boolean canResume = directory.exists();
        DiscoveryBuilder builder = new DiscoveryBuilder(
                iri,
                definition.getApplications(),
                definition.getTransformers());

        if (configuration.levelLimit > -1) {
            builder.setLevelLimit(configuration.levelLimit);
        }
        builder.setRegistry(registry);
        SampleStore store = createSampleStore(registry, directory);
        builder.setFilter(createNodeFilter(store, registry));
        builder.setStore(store);
        builder.setDataSampleTransformer(createDataSampleTransformer(registry));
        builder.setDataset(dataset);
        if (configuration.discoveryTimeLimit > -1) {
            builder.setTimeLimitInMinutes(configuration.discoveryTimeLimit);
        }
        if (configuration.maxNodeExpansionTimeSeconds > -1) {
            builder.setMaxNodeExpansionTimeMs(
                    configuration.maxNodeExpansionTimeSeconds * 1000);
        }
        if (configuration.resume && canResume) {
            return builder.resume(directory);
        } else {
            return builder.createNew();
        }
    }

    protected SampleStore createSampleStore(
            MeterRegistry meterRegistry, File directory) {
        switch (configuration.store) {
            case "memory":
                return SampleStore.memoryStore();
            case "disk":
                return SampleStore.fileSystemStore(
                        new File(directory, "/working/file-store"),
                        meterRegistry);
            case "memory-disk":
                return SampleStore.withCache(
                        SampleStore.memoryStore(),
                        SampleStore.fileSystemStore(
                                new File(directory, "/working/file-store"),
                                meterRegistry));
            default:
                throw new RuntimeException(
                        "Unknown sample store: " + configuration.store);
        }
    }

    protected NodeFilter createNodeFilter(
            SampleStore sampleStore, MeterRegistry meterRegistry) {
        switch (configuration.filter) {
            case "diff":
                return new DiffBasedFilter(sampleStore, meterRegistry);
            case "isomorphic":
                return new Rdf4jIsomorphic(sampleStore, meterRegistry);
            case "no-filter":
                return new NoFilter();
            default:
                throw new RuntimeException("Invalid filter name");
        }
    }

    protected DataSampleTransformer createDataSampleTransformer(
            MeterRegistry meterRegistry) {
        if (configuration.useDataSampleMapping) {
            return DataSampleTransformer.mapStatements(meterRegistry);
        } else {
            return DataSampleTransformer.noAction();
        }
    }

    protected void addResourceStrategy(Discovery discovery) {
        if (!(discovery.getStore() instanceof HierarchicalStore)) {
            return;
        }
        ResourceStrategy resourceStrategy = new ResourceStrategy();
        resourceStrategy.setStore((HierarchicalStore) discovery.getStore());
        resourceStrategy.addHierarchicalStoreOptimization(
                ResourceStrategy.moveNode(() ->
                        ResourceStrategy.memoryUtilizationInPercent() > 0.85));
        resourceStrategy.addHierarchicalStoreOptimization(
                ResourceStrategy.moveNodeAndBigDiff(() ->
                        ResourceStrategy.memoryFreeInMb() < 1024, 1000));
        resourceStrategy.addHierarchicalStoreOptimization(
                ResourceStrategy.moveAll(() ->
                        ResourceStrategy.memoryUtilizationInPercent() > 0.95
                                && ResourceStrategy.memoryFreeInMb() < 512));
        discovery.addListener(resourceStrategy);
    }

    protected void addOptionalListeners(
            LoadRemoteDefinition definition, Discovery discovery) {
        if (configuration.useStrongGroups) {
            PruneByStrongGroup pruneByGroup =
                    new PruneByStrongGroup(definition.getGroups());
            discovery.addListener(pruneByGroup);
        }
    }

}
