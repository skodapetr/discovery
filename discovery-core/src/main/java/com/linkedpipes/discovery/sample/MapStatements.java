package com.linkedpipes.discovery.sample;

import com.linkedpipes.discovery.DiscoveryException;
import com.linkedpipes.discovery.MeterNames;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * When executing SPARQL updates new statements are always created
 * although they have the same content - they are equal.
 *
 * <p>As many transformers do modify only small part of the sample, there
 * is a huge duplicity. We try to mitigate this in this storage.
 *
 * <p>This class can be used to map new statements to already known statements.
 *
 * <p>We also need to map values, as the MemIRI holds a reference
 * to all statements it is used in - preventing them from being GC.
 */
class MapStatements implements DataSampleTransformer {

    private static final Logger LOG =
            LoggerFactory.getLogger(MapStatements.class);

    private Map<Statement, Statement> known = new HashMap<>();

    /**
     * We map IRIs as the MemIRI in rdf4j hold references to all statements,
     * causing them not to be GC.
     */
    private Map<Resource, Resource> knownResources = new HashMap<>();

    private SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();

    private final Timer timer;

    public MapStatements(MeterRegistry registry) {
        this.timer = registry.timer(MeterNames.STORE_MAP_MEMORY);
    }

    @Override
    public List<Statement> transform(List<Statement> statements)
            throws DiscoveryException {
        Instant start = Instant.now();
        addToKnown(statements);
        List<Statement> result = statements.stream()
                .map(st -> known.get(st))
                .collect(Collectors.toList());
        timer.record(Duration.between(start, Instant.now()));
        return result;
    }

    private void addToKnown(List<Statement> statements)
            throws DiscoveryException {
        for (Statement statement : statements) {
            if (known.containsKey(statement)) {
                continue;
            }
            Statement mapped = valueFactory.createStatement(
                    mapResource(statement.getSubject()),
                    (IRI) mapResource(statement.getPredicate()),
                    mapValue(statement.getObject()),
                    mapResource(statement.getContext()));
            known.putIfAbsent(mapped, mapped);
        }
    }

    private Resource mapResource(Resource resource) throws DiscoveryException {
        if (resource == null) {
            return null;
        }
        Resource result = knownResources.get(resource);
        if (result != null) {
            return result;
        }
        if (resource instanceof BNode) {
            throw new DiscoveryException(
                    "We do not support BNodes for mapping.");
        } else if (resource instanceof IRI) {
            IRI iri = valueFactory.createIRI(resource.stringValue());
            knownResources.put(iri, iri);
            return iri;
        } else {
            throw new DiscoveryException(
                    "Unsupported resource type for:"
                            + resource.getClass().getName());
        }
    }

    private Value mapValue(Value value) throws DiscoveryException {
        if (value instanceof Resource) {
            return mapResource((Resource) value);
        }
        if (value instanceof Literal) {
            Literal literal = (Literal) value;
            if (literal.getDatatype() != null) {
                return valueFactory.createLiteral(
                        literal.stringValue(),
                        (IRI) mapResource(literal.getDatatype()));
            }
        }
        return value;
    }

    @Override
    public boolean levelDidEnd(int level) {
        LOG.info("Known statements size: {}", known.size());
        return true;
    }

}
