package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.DiscoveryListener;
import io.micrometer.core.instrument.MeterRegistry;
import org.eclipse.rdf4j.model.Statement;

import java.util.List;

/**
 * Can be used to transform newly created data samples.
 */
public interface DataSampleTransformer extends DiscoveryListener {

    List<Statement> transform(List<Statement> statements) throws DiscoveryException;

    static DataSampleTransformer noAction() {
        return statements -> statements;
    }

    static DataSampleTransformer mapStatements(MeterRegistry registry) {
        return new MapStatements(registry);
    }

}
