package com.linkedpipes.discovery.node;

import com.linkedpipes.discovery.MeterNames;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.Descriptor;
import com.linkedpipes.discovery.model.Feature;
import com.linkedpipes.discovery.model.Transformer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.util.List;
import java.util.stream.Collectors;

public class AskNode {

    private final List<Application> applications;

    private final List<Transformer> transformers;

    private final Timer matchDataTimer;

    public AskNode(
            List<Application> applications,
            List<Transformer> transformers,
            MeterRegistry registry) {
        this.applications = applications;
        this.transformers = transformers;
        this.matchDataTimer = registry.timer(MeterNames.MATCH_DATA);
    }

    public List<Application> matchApplications(Repository repository) {
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

    public List<Transformer> matchTransformer(Repository repository) {
        return transformers.stream()
                .filter((app -> match(repository, app.features)))
                .collect(Collectors.toList());
    }

}
