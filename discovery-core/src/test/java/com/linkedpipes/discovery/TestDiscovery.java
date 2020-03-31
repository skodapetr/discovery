package com.linkedpipes.discovery;

import com.linkedpipes.discovery.filter.DiffBasedFilter;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.store.MemoryStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestDiscovery {

    @Test
    public void testDiscoveryCaseMemoryStore000() throws Exception {
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
                "urn:discovery", "",
                Collections.emptyList(), transformers,
                Collections.emptyList());
        builder.setDataset(dataset);
        builder.setRegistry(registry);
        builder.setStore(store);
        builder.setDataSampleTransformer(
                DataSampleTransformer.mapStatements(registry));
        builder.setFilter(new DiffBasedFilter(store, registry));
        Discovery context = builder.createNew();

        DiscoveryRunner discovery = new DiscoveryRunner();
        discovery.explore(context);

        Node root = context.getRoot();
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

}
