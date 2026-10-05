# New experiments

## Experiment 1: SPACE-TIME TRADE-OFF

Measure the space-time trade-off of the adaptivenss against the eager.
We measure three things. One: simply on case centric we show that the adaptive avoids extra space
that (near-)never-touched pairs take in the eager's index, related to the query latency.
Two: we show adaptive vs eager for all (top-5) perspectives space-time trade off; we need
to show that having materialised all pairs and all perspectives (i.e. eager) is worse (due to disk covegare).
Three: evaluate the attribute embeddings (in the index) cost compared to a join-back from activity index table.

## Experiment 2: SCALE-OUT

(Run on cluster: use docker swarm).
Index and query of a replicated BPIC2017 (sample traces and replicate them with new trace_ids) on cluster. The result we want are two figures.
One figure will contain two plots: show eager (case-centric only) vs adaptive indexing time
and query latency with and without attributes workloads.
The second figure will show the multiperspectiveness distributed performance of the adaptive querying
on different (3) perspectives and attribute-included queries (non-empty ones).
We also need to report shuffle time for group colocation.

## Experiment 3: SENSITIVITY

We need to test a bursty workload to exercise the hysteris bane epsilon, misestimated costs
and repeated drift episodes (in the perspectiveness).

## Experiment 4: QUERY LATENCY VS. PATTERN LENGTH

There is already some code that evaluates structural and attribute-aware queries on adaptive SIESTA, ELK and MR.
We want to repeat this experiment, including the LPG (repo is cloned in graphdb-eventlogs/ dir) as competitor (if easy).
You should be very careful of how you're setting ELK in order to be consistent: each trace id is a document
and each event should include its attributes as well. Alternatively, you can make an event as a document; you pick
the most fair one. But we want case-centric only queries and one for BPIC2011, BPIC2012 BPIC2017, BPIC2015 and BPIC2018.

A complimentary experiment will show query latency against pattern length on attribute aware queries on other perspectives
(rather than case id), e.g. top 3, non-empty queries, well distributed against their grouping property. This will
evaluate adaptive siesta against MR and LPG only (ELK cannot support that).
