package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.sample.DataSampleTransformer;
import com.linkedpipes.discovery.sample.store.SampleGroup;
import com.linkedpipes.discovery.sample.store.SampleRef;
import com.linkedpipes.discovery.sample.store.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.IsolationLevels;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Find find applications and expand given node.
 */
public class ExpandNode {

    private final SampleStore store;

    private final NodeFilter filter;

    private final AskNode askNode;

    private final Timer createRepositoryTimer;

    private final Timer transformDataTimer;

    private DataSampleTransformer dataSampleTransformer;

    public ExpandNode(
            SampleStore store, NodeFilter filter, AskNode askNode,
            DataSampleTransformer dataSampleTransformer,
            MeterRegistry registry) {
        this.store = store;
        this.filter = filter;
        this.askNode = askNode;
        this.dataSampleTransformer = dataSampleTransformer;
        //
        this.createRepositoryTimer = registry.timer(
                MeterNames.CREATE_REPOSITORY);
        this.transformDataTimer = registry.timer(MeterNames.UPDATE_DATA);
    }

    /**
     * Expand node with data sample, for example root.
     */
    public void expandRoot(Node node, List<Statement> dataSample) {
        Repository repository = createRepository(dataSample);
        try {
            expandFromRepository(node, repository);
        } finally {
            repository.shutDown();
        }
        node.setExpanded(true);
    }

    private Repository createRepository(List<Statement> dataSample) {
        Instant start = Instant.now();
        MemoryStore store = new MemoryStore();
        store.setDefaultIsolationLevel(IsolationLevels.NONE);
        Repository repository = new SailRepository(store);
        repository.init();
        try (RepositoryConnection connection = repository.getConnection()) {
            connection.add(dataSample);
        } finally {
            createRepositoryTimer.record(
                    Duration.between(start, Instant.now()));
        }
        return repository;
    }

    /**
     * Set applications and next nodes (with transformers).
     */
    private void expandFromRepository(
            Node node, Repository repository) {
        node.setApplications(askNode.matchApplications(repository));
        List<Transformer> transformers = askNode.matchTransformer(repository);
        node.setNext(createNextLevelNodes(node, transformers));
    }

    private List<Node> createNextLevelNodes(
            Node parent, List<Transformer> transformers) {
        return transformers.stream()
                .map(transformer -> new Node(parent, transformer))
                .collect(Collectors.toList());
    }

    public void expand(Node node) throws DiscoveryException {
        // We need to create data sample, for this node.
        List<Statement> parentDataSample =
                store.load(node.getPrevious().getDataSampleRef());
        Repository repository = createRepository(parentDataSample);
        try {
            var dataSample = transformRepository(
                    repository, node.getTransformer());
            if (filter.isNewNode(node, dataSample)) {
                SampleRef ref = store.store(dataSample, SampleGroup.NODE);
                node.setDataSampleRef(ref);
                expandFromRepository(node, repository);
            } else {
                // We already see this node, there is no need
                // to explore it any further.
                node.setRedundant(true);
            }
        } finally {
            repository.shutDown();
        }
        node.setExpanded(true);
    }

    private List<Statement> transformRepository(
            Repository repository, Transformer transformer)
            throws DiscoveryException {
        Instant start = Instant.now();
        List<Statement> statements = new ArrayList<>();
        try (var connection = repository.getConnection()) {
            Update query = connection.prepareUpdate(
                    transformer.configurationTemplate.query);
            query.execute();
            // Now we collect the statements.
            var result = connection.getStatements(null, null, null);
            while (result.hasNext()) {
                statements.add(result.next());
            }
        }
        transformDataTimer.record(Duration.between(start, Instant.now()));
        return dataSampleTransformer.transform(statements);
    }

}
