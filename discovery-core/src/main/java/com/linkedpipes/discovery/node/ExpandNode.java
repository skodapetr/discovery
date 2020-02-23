package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Descriptor;
import com.linkedpipes.discovery.model.Feature;
import com.linkedpipes.discovery.model.Transformer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryResult;
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

    private final List<Application> applications;

    private final List<Transformer> transformers;

    private final NodeFacade nodeFacade;

    private final Timer createRepositoryTimer;

    private final Timer transformDataTimer;

    private final Timer matchDataTimer;

    public ExpandNode(
            List<Application> applications,
            List<Transformer> transformers,
            NodeFacade nodeFacade,
            MeterRegistry registry) {
        this.applications = applications;
        this.transformers = transformers;
        this.nodeFacade = nodeFacade;
        //
        this.createRepositoryTimer =
                registry.timer(MeterNames.CREATE_REPOSITORY);
        this.transformDataTimer =
                registry.timer(MeterNames.UPDATE_DATA);
        this.matchDataTimer =
                registry.timer(MeterNames.MATCH_DATA);
    }

    public void expand(Node node) throws DiscoveryException {
        Repository repository = createRepository(node);
        try {
            node.setApplications(findApplications(repository));
            node.setNext(findNextNodes(node, repository));
        } finally {
            repository.shutDown();
        }
    }

    private Repository createRepository(Node node) {
        // We do this here to not count time of getting the data.
        List<Statement> dataSample;
        try {
            dataSample = nodeFacade.getDataSample(node);
        } catch (DiscoveryException ex) {
            throw new RuntimeException("Can't create repository.", ex);
        }
        Instant start = Instant.now();
        Repository result = new SailRepository(new MemoryStore());
        result.init();
        try (RepositoryConnection connection = result.getConnection()) {
            connection.add(dataSample);
        } catch (RuntimeException ex) {
            throw new RuntimeException("Can't create repository.", ex);
        } finally {
            createRepositoryTimer.record(
                    Duration.between(start, Instant.now()));
        }
        return result;
    }

    private List<Application> findApplications(Repository repository) {
        return applications.stream()
                .filter((app -> match(repository, app.features)))
                .collect(Collectors.toList());
    }

    private boolean match(Repository repository, List<Feature> features) {
        for (Feature feature : features) {
            for (Descriptor descriptor : feature.descriptors) {
                if (!match(repository, descriptor.query)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean match(Repository repository, String query) {
        return matchDataTimer.record(() -> {
            try (RepositoryConnection connection = repository.getConnection()) {
                return connection.prepareBooleanQuery(query).evaluate();
            }
        });
    }

    private List<Node> findNextNodes(Node node, Repository repository)
            throws DiscoveryException {
        List<Node> result = new ArrayList<>();
        for (Transformer transformer : transformers) {
            if (!match(repository, transformer.features)) {
                continue;
            }
            List<Statement> newSample = transformData(node, transformer);
            result.add(nodeFacade.createNode(node, transformer, newSample));
        }
        return result;
    }

    private List<Statement> transformData(
            Node node, Transformer transformer) {
        Repository repository = createRepository(node);
        return transformDataTimer.record(() -> {
            try (RepositoryConnection connection = repository.getConnection()) {
                Update query = connection.prepareUpdate(
                        transformer.configurationTemplate.query);
                query.execute();
                RepositoryResult<Statement> content =
                        connection.getStatements(null, null, null);
                return resultToList(content);
            }
        });
    }

    private List<Statement> resultToList(RepositoryResult<Statement> result) {
        List<Statement> list = new ArrayList<>();
        while (result.hasNext()) {
            list.add(result.next());
        }
        return list;
    }

}
