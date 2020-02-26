package com.linkedpipes.discovery.cli.factory;

import com.linkedpipes.discovery.Discovery;
import com.linkedpipes.discovery.filter.DiffBasedFilter;
import com.linkedpipes.discovery.filter.NodeFilter;
import com.linkedpipes.discovery.filter.NoFilter;
import com.linkedpipes.discovery.filter.Rdf4jIsomorphic;
import com.linkedpipes.discovery.model.Application;
import com.linkedpipes.discovery.model.ModelAdapter;
import com.linkedpipes.discovery.model.Transformer;
import com.linkedpipes.discovery.rdf.RdfAdapter;
import com.linkedpipes.discovery.rdf.UnexpectedInput;
import com.linkedpipes.discovery.sample.SampleStore;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public abstract class DiscoveryBuilder {

    private static final Logger LOG =
            LoggerFactory.getLogger(DiscoveryBuilder.class);

    private static final String DEFAULT_FILTER_STRATEGY = "diff";

    private String filterStrategy = DEFAULT_FILTER_STRATEGY;

    protected List<Transformer> transformers = new ArrayList<>();

    protected List<Application> applications = new ArrayList<>();

    protected Function<String, SampleStore> storeFactory = null;

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

    public void setFilterStrategy(String filterStrategyName) {
        this.filterStrategy = filterStrategyName;
    }

    public void setStoreFactory(Function<String, SampleStore> storeFactory) {
        this.storeFactory = storeFactory;
    }

    protected NodeFilter createFilterStrategy(
            SampleStore sampleStore, MeterRegistry meterRegistry) {
        switch (filterStrategy) {
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

    public abstract List<Discovery> create(MeterRegistry registry)
            throws Exception;

}
