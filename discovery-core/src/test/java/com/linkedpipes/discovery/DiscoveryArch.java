package com.linkedpipes.discovery;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition;
import com.tngtech.archunit.library.GeneralCodingRules;
import org.slf4j.Logger;

@AnalyzeClasses(
        packages = "com.linkedpipes.discovery..",
        importOptions = ImportOption.DoNotIncludeTests.class)
public class DiscoveryArch {

    @ArchTest
    public final ArchRule noGenericExceptions =
            GeneralCodingRules.NO_CLASSES_SHOULD_THROW_GENERIC_EXCEPTIONS;

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
