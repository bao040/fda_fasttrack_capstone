-- Grain: 1 row per (safetyreportid, reaction). Reports with zero/null reactions
-- are kept (outer flatten) as a single placeholder row with reaction fields
-- NULL, rather than silently dropped — surfaced for DQ completeness tracking.
-- reactionmeddrapt is free-text MedDRA vocabulary (tens of thousands of
-- possible terms) — not an enum, don't test accepted_values on it.
select
    t.safetyreportid,
    r.index as reaction_index,
    r.value:reactionmeddrapt::string as reactionmeddrapt,
    r.value:reactionmeddraversionpt::string as reactionmeddraversionpt,
    r.value:reactionoutcome::int as reactionoutcome
from {{ source('landing', 'drug_adverse_events_reports') }} t,
     lateral flatten(input => t.reaction, outer => true) r