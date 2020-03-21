package com.linkedpipes.discovery;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.conditions.ArchPredicates;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import com.tngtech.archunit.library.GeneralCodingRules;
import org.slf4j.Logger;

import java.util.Iterator;

@AnalyzeClasses(
        packages = "com.linkedpipes.discovery..",
        importOptions = ImportOption.DoNotIncludeTests.class)
public class DiscoveryArch {

    /**
     * We allow generic exceptions in {@link Iterator};
     */
    @ArchTest
    public final ArchRule noGenericExceptions =
            ArchRuleDefinition.noClasses()
                    .that(ArchPredicates.are(DescribedPredicate.not(
                            JavaClass.Predicates.assignableTo(Iterator.class))))
                    .should(GeneralCodingRules.THROW_GENERIC_EXCEPTIONS);

    @ArchTest
    public final ArchRule noJavaUtilLogging =
            GeneralCodingRules.NO_CLASSES_SHOULD_USE_JAVA_UTIL_LOGGING;

    @ArchTest
    public final ArchRule loggersShouldBePrivateStaticFinal =
            ArchRuleDefinition.fields().that().haveRawType(Logger.class)
                    .should().bePrivate()
                    .andShould().beStatic()
                    .andShould().beFinal();

    @ArchTest
    static final ArchRule interfacesNamesShouldNotEndWithTheWordInterface =
            ArchRuleDefinition.noClasses().that().areInterfaces().should()
                    .haveNameMatching(".*Interface");

}
