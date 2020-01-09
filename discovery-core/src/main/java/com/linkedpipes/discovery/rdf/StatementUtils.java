package com.linkedpipes.discovery.rdf;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;

import java.util.ArrayList;
import java.util.List;

public final class StatementUtils {

    public static Resource findOneByClass(
            List<Statement> statements, String className)
            throws UnexpectedInput {
        List<Resource> resources = findByClass(statements, className);
        if (resources.size() != 1) {
            throw new UnexpectedInput(
                    "Expected only one instance of {} but {} found.",
                    className, resources.size());
        }
        return resources.get(0);
    }

    public static List<Resource> findByClass(
            List<Statement> statements, String className) {
        List<Resource> result = new ArrayList<>();
        for (Statement statement : statements) {
            if (!RDF.TYPE.equals(statement.getPredicate())) {
                continue;
            }
            if (!statement.getObject().stringValue().equals(className)) {
                continue;
            }
            if (result.contains(statement.getSubject())) {
                continue;
            }
            result.add(statement.getSubject());
        }
        return result;
    }

}
