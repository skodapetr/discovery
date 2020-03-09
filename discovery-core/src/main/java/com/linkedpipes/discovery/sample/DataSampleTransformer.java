package com.linkedpipes.discovery.sample;

import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.Statement;

import java.util.List;

/**
 * Can be used to transform newly created data samples.
 */
public interface DataSampleTransformer {

    List<Statement> transform(List<Statement> statements);

    default void logAfterLevelFinished() {
        // Do nothing.
    }

    static DataSampleTransformer noAction() {
        return statements -> statements;
    }

    static DataSampleTransformer mapStatements(MeterRegistry registry) {
        return new MapStatements(registry);
    }

}
