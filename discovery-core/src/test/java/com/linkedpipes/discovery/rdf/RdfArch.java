package com.linkedpipes.discovery.rdf;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;

@AnalyzeClasses(
        packages = "com.linkedpipes.discovery.rdf..",
        importOptions = ImportOption.DoNotIncludeTests.class)
public class RdfArch {

    @ArchTest
    public static final ArchRule noOutsideProjectDependencies =
            ArchRuleDefinition.classes()
                    .that()
                    .resideInAPackage("com.linkedpipes.discovery.rdf..")
                    .should()
                    .onlyDependOnClassesThat()
                    .resideInAnyPackage(getAllowedDependencies())
                    .andShould()
                    .onlyHaveDependentClassesThat()
                    .resideInAPackage("com.linkedpipes.discovery.rdf..");

    private static final String[] getAllowedDependencies() {
        return new String[]{
                "com.linkedpipes.discovery.rdf..",
                "org.eclipse.rdf4j..",
                "org.slf4j..",
                "java.."
        };
    }

}
