# Quiz Log

Spaced-repetition log of end-of-session quiz questions. At the START of each
session, Claude re-asks every question not marked ✅, with a fresh variation.
Mark ✅ only after Sakshyam answers correctly in his own words, unprompted.

## Phase 0 (2026-07-13)

- [ ] **Q1 — grain boundaries:** "Handle changes per hour" isn't in `fct_events`
  (grain excludes identity events). Where is it answerable, and what's the rule
  when you want a new grain as a first-class fact?
- [ ] **Q2 — dedup key:** Why is `time_us` needed in the `fct_events` uniqueness
  key, and in what scenario does it *cause* duplicates instead of preventing them?
- [ ] **Q3 — SCD2 mechanics:** On a handle change: what happens to the old
  `dim_user` row, what does the new row contain, and which row does an event
  from last week join to (and via what join condition)?
