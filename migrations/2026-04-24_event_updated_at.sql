-- Migration: add auto-update trigger for event.updated_at
-- Applied on prod on earlier date, now backfilled to canonical.

CREATE OR REPLACE FUNCTION public.set_event_updated_at()
    RETURNS trigger
    LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$function$;

DROP TRIGGER IF EXISTS trg_event_set_updated_at ON event;

CREATE TRIGGER trg_event_set_updated_at
    BEFORE UPDATE ON event
    FOR EACH ROW
    EXECUTE FUNCTION set_event_updated_at();
