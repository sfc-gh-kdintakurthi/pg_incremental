ALTER TABLE incremental.sequence_pipelines ADD COLUMN max_batch_size bigint;

DROP FUNCTION incremental.create_sequence_pipeline(text,regclass,text,text,bool);
CREATE FUNCTION incremental.create_sequence_pipeline(
    pipeline_name text,
    source_table_name regclass,
    command text,
    schedule text default '* * * * *',
    max_batch_size bigint default NULL,
    execute_immediately bool default true)
 RETURNS void
 LANGUAGE C
AS 'MODULE_PATHNAME', $function$incremental_create_sequence_pipeline$function$;
COMMENT ON FUNCTION incremental.create_sequence_pipeline(text,regclass,text,text,bigint,bool)
 IS 'create a pipeline of new sequence ranges';
