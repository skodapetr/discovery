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
- *-e*, *--experiment* - URL of experiment specification. Applications
    nad transformers are loaded from the experiment.
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

[Java]: <http://www.oracle.com/technetwork/java/javase/downloads/index.html>
[Git]: <https://git-scm.com/>
[Maven]: <https://maven.apache.org/>
