# AWS SRO Data Pipeline

AWS Data Engineering proof of concept for processing insurance policies (*apólices*) and endorsements (*endossos*).

The pipeline uses Amazon EventBridge Scheduler to trigger AWS Step Functions, which orchestrates eight sequential AWS Glue jobs. Data is organized into landing, bronze, silver, and gold layers in Amazon S3, with AWS Glue Data Catalog and Amazon Athena supporting data validation.

## Architecture

![AWS SRO Pipeline Architecture](sro-pipeline-architecture.png)

[View editable diagram (SVG)](sro-pipeline-architecture.svg)


- **Scheduling:** Amazon EventBridge Scheduler
- **Orchestration:** AWS Step Functions — `sfn_sro_pipeline`
- **Processing:** Eight sequential AWS Glue jobs
- **Storage:** Amazon S3 — `dgo-sro-data-lake`
- **Metadata:** AWS Glue Data Catalog — `sro_catalog`
- **Validation:** Amazon Athena

## Processing Sequence

| Step | AWS Glue Job |
| --- | --- |
| 1 | `job_landing_bronze_apolice` |
| 2 | `job_bronze_silver_apolice` |
| 3 | `job_silver_gold_apolice` |
| 4 | `job_gold_json_sro0001` |
| 5 | `job_landing_bronze_endosso` |
| 6 | `job_bronze_silver_endosso` |
| 7 | `job_silver_gold_endosso` |
| 8 | `job_gold_json_sro0002` |

## Data Lake Layers

Both entities follow the same logical progression:

`landing → bronze → silver → gold`

The final job for each entity generates its corresponding JSON: SRO0001 for policies and SRO0002 for endorsements.

## JSON Outputs

The final jobs write simplified POC JSON documents to Amazon S3:

| Output | S3 Location |
| --- | --- |
| SRO0001 — Policies | `s3://dgo-sro-data-lake/gold/json/sro0001/SRO0001.json` |
| SRO0002 — Endorsements | `s3://dgo-sro-data-lake/gold/json/sro0002/SRO0002.json` |

These paths are confirmed by the exported Glue scripts.

## Repository Contents

- [Glue job scripts](glue/jobs/)
- [Step Functions definition](step-functions/sfn_sro_pipeline.asl.json)
- [EventBridge Scheduler configuration](eventbridge/README.md)
- [Execution evidence](docs/evidence/)

## Scope

This repository includes the POC architecture, exported Glue scripts,
Step Functions definition, scheduling documentation, and execution evidence.

The JSON documents are simplified POC outputs. Regulatory compliance
and production readiness have not been established.

## Execution Evidence

The pipeline completed successfully on October 17, 2026.
All eight AWS Glue tasks succeeded, with a total orchestration
duration of 15 minutes and 20 seconds.

### Execution Summary

![Successful execution summary](docs/evidence/step-functions-execution-summary.png)

### Completed Workflow

![All eight tasks completed successfully](docs/evidence/step-functions-execution-graph.png)

These screenshots demonstrate successful orchestration.
Data output validation is documented separately when available.
