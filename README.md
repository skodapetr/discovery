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
- *-e*, *--experiment* - URL of an experiment specification. Experiment
   consists of multiple discoveries.
- *-d*, *--discovery* - URL of discovery specification. A dataset, 
    applications and transformers are loaded from the discovery definition.
- *--dataset* - Path to dataset directory on local file system, 
    can't be used together with *experiment*.
- *--applications* - Directory with applications to add to the run.
- *--transformers* - Directory with transformers to add to the run.
- *-o*, *--output* - Output directory. 
- *--filter* - Name of filter to use. Available values:
    - *no-filter* - No filter, explore all states. 
    - *isomorphic* - Use RDF4J isomorphic to compare states. 
    - *diff* -  Use RDF4J isomorphic to compare diffs of states.
        Currently this is the fastest available filter and is used by default.
- *--limit* - Number of iterations to explore, -1 or not provided, to 
    run without a limit. 
- *--IHaveBadDiscoveryDefinition* - Can be used to ignore issues with Discovery 
    definition, can be used only with *-d*/*--discovery* option.
- *--UseMapping* - Can be used to map results from SPARQL construct. This
    may reduce memory usage, but require extra CPU to perform the mappings.
- *-store* - Name of store strategy  used to store statements. Available values:
    - *memory* - Store all in memory, fastest store using most memory.
    - *diff* - Store statements in form of diffs from the root. Similar
        to *diff* filter. 
    - *disk* - Store all samples into files. Slowest but with smallest memory
        consumption.
    - *memory-disk* - Use a memory store for filter data and a disk
        store for data samples. If 85% of memory is used, then all data are 
        moved to the disk store and the memory store is no longer used.

[Java]: <http://www.oracle.com/technetwork/java/javase/downloads/index.html>
[Git]: <https://git-scm.com/>
[Maven]: <https://maven.apache.org/>
