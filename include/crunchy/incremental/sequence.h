#pragma once

void		InitializeSequencePipelineState(char *pipelineName, Oid sequenceId, int64 maxBatchSize);
void		UpdateLastProcessedSequenceNumber(char *pipelineName, int64 lastSequenceNumber);
void		ExecuteSequenceRangePipeline(char *pipelineName, char *command);
Oid			FindSequenceForRelation(Oid relationId);
