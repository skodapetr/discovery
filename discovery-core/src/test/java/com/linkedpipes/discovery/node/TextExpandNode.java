package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.TestResources;
import com.linkedpipes.discovery.filter.NoFilter;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Dataset;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.store.SampleGroup;
import com.linkedpipes.discovery.sample.store.SampleStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TextExpandNode {

    private final SimpleMeterRegistry memoryRegistry =
            new SimpleMeterRegistry();

    @Test
    public void findDCTermsInNkod()
            throws UnexpectedInput, IOException, DiscoveryException {
        List<Application> applications = Arrays.asList(
                ModelAdapter.loadApplication(TestResources.asStatements(
                        "pipeline/application/dcterms.ttl")));
        SampleStore sampleStore = SampleStore.memoryStore();
        ExpandNode expander = new ExpandNode(
                "", sampleStore,
                new NoFilter(),
                new AskNode(
                        applications, Collections.emptyList(), memoryRegistry),
                DataSampleTransformer.noAction(),
                memoryRegistry);

        Dataset dataset = ModelAdapter.loadDataset(
                "http://nkod",
                "NKOD",
                TestResources.file("pipeline/dataset/nkod"));

        Node root = new Node("root");
        expander.expandRoot(root, dataset.sample);

        Assertions.assertEquals(0, root.getNext().size());
        Assertions.assertEquals(1, root.getApplications().size());
        Assertions.assertEquals(
                "https://discovery.linkedpipes.com/resource/application/"
                        + "dcterms/template",
                root.getApplications().get(0).iri);
    }

    @Test
    public void findTimelineInNkod()
            throws UnexpectedInput, IOException, DiscoveryException {
        List<Application> applications = Arrays.asList(
                ModelAdapter.loadApplication(TestResources.asStatements(
                        "pipeline/application/dcterms.ttl")),
                ModelAdapter.loadApplication(TestResources.asStatements(
                        "pipeline/application/timeline.ttl")));

        List<Transformer> transformers = Arrays.asList(
                ModelAdapter.loadTransformer(TestResources.asStatements(
                        "pipeline/transformer/"
                                + "dcterms-issued-to-dcterms-date.ttl")));

        SampleStore sampleStore = SampleStore.memoryStore();
        ExpandNode expander = new ExpandNode(
                "", sampleStore,
                new NoFilter(),
                new AskNode(applications, transformers, memoryRegistry),
                DataSampleTransformer.noAction(),
                memoryRegistry);


        Dataset dataset = ModelAdapter.loadDataset(
                "http://nkod",
                "NKOD",
                TestResources.file("pipeline/dataset/nkod"));

        Node root = new Node("root");
        expander.expandRoot(root, dataset.sample);
        root.setDataSampleRef(
                sampleStore.store(dataset.sample, SampleGroup.ROOT));

        Assertions.assertEquals(1, root.getNext().size());
        Assertions.assertEquals(1, root.getApplications().size());

        Node next = root.getNext().get(0);
        expander.expand(next);
        Assertions.assertEquals(0, next.getNext().size());
        Assertions.assertEquals(2, next.getApplications().size());
        Assertions.assertEquals(
                "https://discovery.linkedpipes.com/resource/transformer/"
                        + "dcterms-issued-to-dcterms-date/template",
                next.getTransformer().iri);
    }

}
