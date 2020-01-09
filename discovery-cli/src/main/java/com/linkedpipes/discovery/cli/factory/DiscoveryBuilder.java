package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.filter.DiffBasedFilter;
import com.linkedpipes.discovery.filter.FilterStrategy;
import com.linkedpipes.discovery.filter.NoFilter;
import com.linkedpipes.discovery.filter.Rdf4jIsomorphic;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public abstract class DiscoveryBuilder {

    private static final Logger LOG =
            LoggerFactory.getLogger(DiscoveryBuilder.class);

    private static final String DEFAULT_FILTER_STRATEGY = "diff";

    private String filterStrategyName = DEFAULT_FILTER_STRATEGY;

    protected List<Transformer> transformers = new ArrayList<>();

    protected List<Application> applications = new ArrayList<>();

    public void addApplications(File directory) throws IOException {
        Files.walk(directory.toPath())
                .filter((file) -> file.toFile().isFile())
                .forEach((file) -> {
                    try {
                        applications.add(ModelAdapter.loadApplication(
                                RdfAdapter.asStatements(file.toFile())));
                    } catch (UnexpectedInput | IOException ex) {
                        LOG.error("Can't load application from: {}", file, ex);
                    }
                });
    }

    public void addTransformers(File directory) throws IOException {
        Files.walk(directory.toPath())
                .filter((file) -> file.toFile().isFile())
                .forEach((file) -> {
                    try {
                        transformers.add(ModelAdapter.loadTransformer(
                                RdfAdapter.asStatements(file.toFile())));
                    } catch (UnexpectedInput | IOException ex) {
                        LOG.error("Can't load transformer from: {}", file, ex);
                    }
                });
    }

    public void setFilterStrategy(String name) {
        filterStrategyName = name;
    }

    protected FilterStrategy getFilterStrategy(MeterRegistry meterRegistry) {
        switch (filterStrategyName) {
            case "diff":
                return new DiffBasedFilter(meterRegistry);
            case "isomorphic":
                return new Rdf4jIsomorphic(meterRegistry);
            case "no-filter":
                return new NoFilter();
            default:
                throw new RuntimeException("Invalid filter name");
        }
    }

    public abstract Discovery create(MeterRegistry registry) throws Exception;

}
