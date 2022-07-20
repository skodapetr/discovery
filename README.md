# Deprecated
This repository is no longer maintained.

# Visualization discovery

## Requirements
- [Java] 11 or 13
- [Git]
- [Maven], 3.2.5 or newer

## How to build
```
$ git clone https://github.com/skodapetr/discovery.git
$ cd discovery
$ mvn install
```

## How to run 
```
$ cd deploy
$ java -jar discovery.jar -d {path to discovery IRI}
```

### Arguments
- *-e*, *--Experiment* - URL of an experiment specification. Experiment
   consists of multiple discoveries.
- *-d*, *--Discovery* - URL of discovery specification. A dataset, 
    applications and transformers are loaded from the discovery definition.
- *-o*, *--Output* (**default**: *./output*) - Output directory. 
- *--Filter* (**default**: *diff*) - Name of filter to use. Available values:
    - *no-filter* - No filter, explore all states. 
    - *isomorphic* - Use RDF4J isomorphic to compare states. 
    - *diff* - Use RDF4J isomorphic to compare diffs of states.
- *--LevelLimit* - Number of iterations to explore, -1 or not provided, to 
    run without a limit. 
- *--IHaveBadDiscoveryDefinition* - Can be used to ignore issues with Discovery 
    definition, can be used only with *-d*/*--discovery* option.
- *--UseMapping* - Can be used to map results from SPARQL construct. This
    highly reduce memory usage, but require extra CPU to perform the mappings.
    Use of this option is recommended for most scenarios.
- *--Store* (**default**: *memory*) - Name of store strategy  used to store statements. Available values:
    - *memory* - Store all in memory, fastest store using most memory.
    - *disk* - Store all samples into files. Slowest but with smallest memory
        consumption.
    - *memory-disk* - Combine memory and disk store.
- *--Resume* - When set we try to resume discoveries from output directory.
- *--DiscoveryTimeLimit* - Specify discovery time limit in minutes.
- *--StrongGroups* - When set use strong transformer groups. Whole 
        group is explored first before other transformers can be applied.  
- *--UrlCache* - Can be used to locally remote data to speed up loading
        of definitions on slower internet connections. 

Arguments can also be defined as a part of a discovery/experiment
as shown in the following bellow:
```
<urn:discovery> a <https://discovery.linkedpipes.com/vocabulary/discovery/Input> ;
    <urn:DiscoveryConfiguration> [
        <urn:filter> "diff" ;
        <urn:store> "memory" ;
        <urn:useDataSampleMapping> true ;
        <urn:useStrongGroups> true 
    ] ;

<urn:expeeriment> a <https://discovery.linkedpipes.com/vocabulary/experiment/Experiment> ;
    <urn:DiscoveryConfiguration> [
        <urn:output> "./output" ;
    ] ;
.
```
The experiment configuration supports only *Output* predicate.
Discovery supports all arguments except *IHaveBadDiscoveryDefinition*.
If a discovery runs as a part of an experiment, the discovery *Output*
predicate is ignored. The arguments defined on command line do override
arguments defined as a part of a discovery definition. 
        
[Java]: <http://www.oracle.com/technetwork/java/javase/downloads/index.html>
[Git]: <https://git-scm.com/>
[Maven]: <https://maven.apache.org/>
