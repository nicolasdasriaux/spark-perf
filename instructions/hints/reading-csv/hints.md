# Reading CSV

**Warning icons** :warning: signal elements that are important to be aware of.

**SQL**

![Queries](queries.png)

**Spark Jobs**

![Jobs](jobs.png)

**Stages for All Jobs**

![Stages](stages.png)

## Non-Multiline CSV

**Details for Query 2**

![Non-Multiline CSV Reading Plan](non-multiline-reading-plan.png)

Query 2 - Job 2 \
**Details for Job 2**

![Non-Multiline CSV Reading Job](non-multiline-reading-job.png)

## Multiline CSV

**Details for Query 3**

![Multiline CSV Reading Plan](multiline-reading-plan.png)

Query 3 - Job 3 \
**Details for Job 3**

![Multiline CSV Reading Job](multiline-reading-job.png)

## Schema Inference

**Details for Query 4**

![CSV with Schema Inference Checking Plan](schema-inference-checking-plan.png)

Query 4 - Job 4 - Stage 4 \
**Details for Stage 4**

![CSV with Schema Inference Checking Job](schema-inference-checking-job.png)

No Query - Job 5 - Stage 5 \
**Details for Stage 5**

![CSV with Schema Inference Inferring Job](schema-inference-inferring-job.png)

**Details for Query 5**

![CSV with Schema Inference Reading Job](schema-inference-reading-plan.png)

Query 5 - Job 6 - Stage 6 \
**Details for Stage 6**

![CSV with Schema Inference Reading Job](schema-inference-reading-job.png)
