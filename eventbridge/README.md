# Pipeline Scheduling

Amazon EventBridge Scheduler starts the SRO pipeline by invoking AWS Step Functions.

| Setting | Value |
| --- | --- |
| Schedule name | `schedule_sro_pipeline` |
| Schedule group | `default` |
| Schedule type | One-time |
| Scheduled time | October 17, 2026, at 16:30 |
| Time zone | `America/Sao_Paulo` (UTC−03:00 on the scheduled date) |
| Target service | AWS Step Functions |
| Target state machine | `sfn_sro_pipeline` |
| Target API | `StartExecution` |
| Input payload | `{}` |
| Action after completion | `DELETE` |

## Scope

This schedule was configured for a single POC run, rather than recurring daily processing.

The `DELETE` setting applies to the schedule after completion.

This document records the configuration shown in the AWS Console. It is not a deployment template and does not confirm the schedule's current status.
