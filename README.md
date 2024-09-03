# Remix

This is the artifact (demo) of "Multi-Grained Specifications for Distributed System Model Checking and Verification" presented at EuroSys'25.

**Remix** is a tooling support for the verification using multi-grained specifications. It interfaces the TLC model checker, and provides conformance checking and deterministic execution of model-level traces.



## Overview

### Folder Structure

```
root
|---- apache-zookeeper-3.9.1			(Source code: target distributed system)
|---- checker					(Source code: the checker that replays model-level traces in implementation)

|---- generator					(Model-level scripts: generating model-level traces and parsing format)
	|---- Zab-simulate.ini	    		(Configurations for the model checking)
	|---- generate_trace.sh	    		(Generate model-level traces using TLC. Still in raw data)
	|---- read_trace.sh			(Parse trace format for later processing)
	|---- run.sh				(generate_trace.sh && read_trace.sh)
|---- scripts					(Code-level scripts: building the checker and conducting the trace replay)
	|---- build.sh			    	(Build the checker)
	|---- replay.sh				(Replay given traces in the implementation)
	|---- buildAndReplay.sh     		(Generate traces using TLC and replay them in the implementation)
	|---- checkAndReplay.sh	    		(Generate traces using TLC and replay them in the implementation.
							Pre-requisite: the checker has already been built before)
	|---- stop.sh				(Directly kill all currently running zookeeper processes)
			
|---- output					(Data dir: Model-checking results and statistics generated by TLC)
	|---- *.csv				(Summary of the model checking results)
	|---- trace in raw data			(Sequences of states generated by TLC)
|---- traces					(Data dir: Model-level traces in multiple formats for later processing)
	|---- mck trace dir 			(A batch of traces generated from model checking)
		|---- * (no suffix)	    	(Raw data of traces generated by TLC)
		|---- *.json			(The input file for the checker)
		|---- *.txt			(A format with high readability)
|---- results					(Data dir: Results of deterministic replay)
	|---- mck trace result dir 		(Results of a batch of traces)
		|---- *.out			(Log for debugging)
		|---- matchReport		(Report on whether any discrepancy has been found)
		|---- bugReport			(Report on whether any violation has been detected by the checker)
		|---- trace results		(Results for each trace)
			|---- execution	    	(event sequence that has been walked)
			|---- statistics    	(Checking results of each step)
			|---- nodes		(Runtime data for each node, e.g., conf, data, log, etc)
```



### Pre-requisites for use

* Java 11+
* Python 3+
* [Apache Maven](http://maven.apache.org/) 3.5+

## Quick Start

Build the checker:

```bash
cd script
./build.sh 
```

Replay the traces:

```bash
./replay.sh demo 		
```

Or directly use the following script:

```bash
./buildAndReplay.sh demo 		
```


## Contributing

We appreciate all feedback and contributions. Please use Github issues for user questions and bug reports.
