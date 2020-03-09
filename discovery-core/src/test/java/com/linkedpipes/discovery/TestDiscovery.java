package com.linkedpipes.discovery;

import com.linkedpipes.discovery.filter.DiffBasedFilterForDiffStore;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.node.Node;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.DiffStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestDiscovery {

    /**
     * This test is motivated by initial implementation of
     * DiffBasedFilterForDiffStore, that originally filter all from level 2.
     */
    @Test
    public void testDiscoveryCase000() throws Exception {
        List<Transformer> transformers = Arrays.asList(
                ModelAdapter.loadTransformer(TestResources.asStatements(
                        "pipeline/transformer/"
                                + "dce-to-dcterms-title.ttl")),
                ModelAdapter.loadTransformer(TestResources.asStatements(
                        "pipeline/transformer/"
                                + "geo-pos-to-schema-geocoordinates.ttl")));
        MeterRegistry registry = new SimpleMeterRegistry();
        DiffStore store = new DiffStore(true, registry);
        DiffBasedFilterForDiffStore filter =
                new DiffBasedFilterForDiffStore(store, registry);

        Dataset dataset = ModelAdapter.loadDataset(
                "urn:dataset",
                "000",
                TestResources.file("pipeline/dataset/000"));

        Discovery discovery = new Discovery(
                "urn:discovery",
                dataset,
                transformers,
                Collections.emptyList(),
                filter,
                store,
                600,
                DataSampleTransformer.noAction(),
                registry);

        Node root = discovery.explore(-1);
        List<Node> allNodes = new ArrayList<>();
        List<Node> redundantNodes = new ArrayList<>();
        root.accept((node) -> {
            allNodes.add(node);
            if (node.isRedundant()) {
                redundantNodes.add(node);
            }
        });

        Assertions.assertEquals(5, allNodes.size());
        Assertions.assertEquals(1, redundantNodes.size());
    }

}
