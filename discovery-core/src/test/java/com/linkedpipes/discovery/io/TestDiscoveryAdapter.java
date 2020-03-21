package com.linkedpipes.discovery.io;

import com.linkedpipes.discovery.DiscoveryRunner;
import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.DiscoveryBuilder;
import com.linkedpipes.discovery.TestResources;
import com.linkedpipes.discovery.filter.DiffBasedFilter;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.store.MemoryStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestDiscoveryAdapter {

    @Test
    public void saveAndLoad() throws Exception {
        DiscoveryRunner discovery = new DiscoveryRunner();

        DiscoveryBuilder limitedBuilder = createContext();
        limitedBuilder.setLevelLimit(1);
        Discovery tillLevelOne = limitedBuilder.createNew();
        discovery.explore(tillLevelOne);

        Discovery continueContext;
        File directory = Files.createTempDirectory("discovery-test-").toFile();
        try {
            DiscoveryAdapter adapter = new DiscoveryAdapter();
            adapter.save(tillLevelOne, directory);

            DiscoveryBuilder continueBuilder = createContext();
            continueContext = continueBuilder.resume(directory);
        } finally {
            FileUtils.deleteDirectory(directory);
        }
        discovery.explore(continueContext);

        Node root = continueContext.getRoot();
        List<Node> allExpanded = new ArrayList<>();
        List<Node> redundantNodes = new ArrayList<>();
        root.accept((node) -> {
            if (node.isExpanded()) {
                allExpanded.add(node);
            }
            if (node.isRedundant()) {
                redundantNodes.add(node);
            }
        });
        Assertions.assertEquals(5, allExpanded.size());
        Assertions.assertEquals(1, redundantNodes.size());
    }

    public DiscoveryBuilder createContext()
            throws UnexpectedInput, IOException {
        List<Transformer> transformers = Arrays.asList(
                ModelAdapter.loadTransformer(TestResources.asStatements(
                        "pipeline/transformer/"
                                + "dce-to-dcterms-title.ttl")),
                ModelAdapter.loadTransformer(TestResources.asStatements(
                        "pipeline/transformer/"
                                + "geo-pos-to-schema-geocoordinates.ttl")));
        MeterRegistry registry = new SimpleMeterRegistry();
        MemoryStore store = new MemoryStore();

        Dataset dataset = ModelAdapter.loadDataset(
                "urn:dataset",
                "000",
                TestResources.file("pipeline/dataset/000"));

        DiscoveryBuilder builder = new DiscoveryBuilder(
                "urn:discovery", Collections.emptyList(), transformers);
        builder.setDataset(dataset);
        builder.setRegistry(registry);
        builder.setStore(store);
        builder.setDataSampleTransformer(
                DataSampleTransformer.mapStatements(registry));
        builder.setFilter(new DiffBasedFilter(store, registry));
        return builder;
    }

}
