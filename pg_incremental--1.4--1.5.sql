-- The event trigger and its function live outside the extension so they survive
-- DROP EXTENSION and can clean up after it. To update them we must drop and
-- recreate them, then immediately remove them from the extension again.
DROP EVENT TRIGGER incremental_drop_extension_trigger;
DROP FUNCTION incremental._drop_extension_trigger();

CREATE FUNCTION incremental._drop_extension_trigger()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SET search_path = pg_catalog
AS $function$
DECLARE
    obj record;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
        IF obj.object_identity = 'pg_incremental' AND obj.object_type = 'extension' THEN
            IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_cron') THEN
                PERFORM cron.unschedule(jobname) FROM cron.job WHERE jobname LIKE 'pipeline:%';
            END IF;
            DROP SCHEMA incremental CASCADE;
        END IF;
    END LOOP;
END;
$function$;

CREATE EVENT TRIGGER incremental_drop_extension_trigger
 ON sql_drop
 WHEN TAG IN ('DROP EXTENSION')
 EXECUTE FUNCTION incremental._drop_extension_trigger();

ALTER EXTENSION pg_incremental DROP EVENT TRIGGER incremental_drop_extension_trigger;
ALTER EXTENSION pg_incremental DROP FUNCTION incremental._drop_extension_trigger();
