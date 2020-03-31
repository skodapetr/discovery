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
$ java -jar discovery.jar
```

### Arguments
- *-e*, *--Experiment* - URL of an experiment specification. Experiment
   consists of multiple discoveries.
- *-d*, *--Discovery* - URL of discovery specification. A dataset, 
    applications and transformers are loaded from the discovery definition.
- *-o*, *--Output* - Output directory. 
- *--Filter* - Name of filter to use. Available values:
    - *no-filter* - No filter, explore all states. 
    - *isomorphic* - Use RDF4J isomorphic to compare states. 
    - *diff* -  Use RDF4J isomorphic to compare diffs of states.
- *--LevelLimit* - Number of iterations to explore, -1 or not provided, to 
    run without a limit. 
- *--IHaveBadDiscoveryDefinition* - Can be used to ignore issues with Discovery 
    definition, can be used only with *-d*/*--discovery* option.
- *--UseMapping* - Can be used to map results from SPARQL construct. This
    may reduce memory usage, but require extra CPU to perform the mappings.
- *--Store* - Name of store strategy  used to store statements. Available values:
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
        
Arguments can also be defined as a part of a discovery as shown in the 
following bellow:
```
<urn:discovery> a <https://discovery.linkedpipes.com/vocabulary/discovery/Input> ;
    <urn:DiscoveryConfiguration> [
        <urn:output> "./output" ;
        <urn:filter> "diff" ;
        <urn:store> "memory" ;
        <urn:useMapping> true ;
        <urn:strongGroups> true 
    ] ;
.
```
Other arguments can be used in similar way except *IHaveBadDiscoveryDefinition*.
The arguments defined on command line do override arguments defined as 
a part of a discovery definition.
        
[Java]: <http://www.oracle.com/technetwork/java/javase/downloads/index.html>
[Git]: <https://git-scm.com/>
[Maven]: <https://maven.apache.org/>
