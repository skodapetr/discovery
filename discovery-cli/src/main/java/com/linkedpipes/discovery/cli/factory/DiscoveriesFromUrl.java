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
                String name, File directory, Dataset dataset,
                boolean resumed, Discovery discoveryContext)
                throws DiscoveryException;

    }

    protected String discoveryUrl;

    public DiscoveriesFromUrl(String discoveryUrl) {
        this.discoveryUrl = discoveryUrl;
    }

    public void create(
            BuilderConfiguration configuration,
            MeterRegistry registry,
            Handler handler) throws Exception {
        RemoteDefinition definition = new RemoteDefinition(
                configuration, discoveryUrl,
                createUrlCache(configuration));
        definition.load();
        // Prepare configuration to use.
        BuilderConfiguration effectiveConfiguration =
                configuration.mergeWithRuntime(
                        definition.getRuntimeConfiguration());
        List<Dataset> datasets = definition.getDatasets();
        // We force same ordering to allow use of resume.
        datasets.sort(Comparator.comparing(dataset -> dataset.iri));
        for (int index = 0; index < datasets.size(); ++index) {
            String name = "discovery_" + String.format("%03d", index);
            File directory = new File(effectiveConfiguration.output, name);
            DiscoveryBuilder builder = createDiscoveryBuilder(
                    directory, datasets.get(index),
                    definition, registry, effectiveConfiguration);
            //
            Discovery discovery;
            boolean resume = configuration.resume && directory.exists();
            if (resume) {
                discovery = builder.resume(directory);
            } else {
                discovery = builder.createNew();
            }
            addResourceStrategy(discovery);
            addOptionalListeners(
                    definition, discovery, effectiveConfiguration);
            handler.handle(
                    name, directory, datasets.get(index), resume, discovery);
        }
    }

    private UrlCache createUrlCache(BuilderConfiguration configuration) {
        if (configuration.urlCache == null) {
            return UrlCache.noCache();
        } else {
            return UrlCache.fileCache(configuration.urlCache);
        }
    }

    private DiscoveryBuilder createDiscoveryBuilder(
            File directory, Dataset dataset,
            RemoteDefinition definition, MeterRegistry registry,
            BuilderConfiguration configuration) {
        DiscoveryBuilder builder = new DiscoveryBuilder(
                discoveryUrl, getDiscoveryNodePrefix(),
                definition.getApplications(),
                definition.getTransformers(),
                definition.getGroups());
        if (configuration.levelLimit != null
                && configuration.levelLimit > -1) {
            builder.setLevelLimit(configuration.levelLimit);
        }
        builder.setRegistry(registry);
        SampleStore store =
                createSampleStore(registry, directory, configuration);
        builder.setFilter(createNodeFilter(store, registry, configuration));
        builder.setStore(store);
        builder.setDataSampleTransformer(
                createDataSampleTransformer(registry, configuration));
        builder.setDataset(dataset);
        if (configuration.discoveryTimeLimit != null
                && configuration.discoveryTimeLimit > -1) {
            builder.setTimeLimitInMinutes(configuration.discoveryTimeLimit);
        }
        if (configuration.maxNodeExpansionTimeSeconds != null
                && configuration.maxNodeExpansionTimeSeconds > -1) {
            builder.setMaxNodeExpansionTimeMs(
                    configuration.maxNodeExpansionTimeSeconds * 1000);
        }
        return builder;
    }

    protected String getDiscoveryNodePrefix() {
        return "";
    }

    protected SampleStore createSampleStore(
            MeterRegistry meterRegistry, File directory,
            BuilderConfiguration configuration) {
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
                        "Unknown sample store: '" + configuration.store + "'");
        }
    }

    protected NodeFilter createNodeFilter(
            SampleStore sampleStore, MeterRegistry meterRegistry,
            BuilderConfiguration configuration) {
        switch (configuration.filter) {
            case "diff":
                return new DiffBasedFilter(sampleStore, meterRegistry);
            case "isomorphic":
                return new Rdf4jIsomorphic(sampleStore, meterRegistry);
            case "no-filter":
                return new NoFilter();
            default:
                throw new RuntimeException(
                        "Invalid filter: '" + configuration.filter + "'");
        }
    }

    protected DataSampleTransformer createDataSampleTransformer(
            MeterRegistry meterRegistry, BuilderConfiguration configuration) {
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
            RemoteDefinition definition, Discovery discovery,
            BuilderConfiguration configuration) {
        if (configuration.useStrongGroups) {
            PruneByStrongGroup pruneByGroup =
                    new PruneByStrongGroup(definition.getGroups());
            discovery.addListener(pruneByGroup);
            // We also need to apply this on the the root.
            pruneByGroup.nodeDidExpand(discovery.getRoot());
        }
    }

}
