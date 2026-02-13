# Project Orchestration Skill

Coordinate multi-agent graph analysis pipeline for Alberta ministry lineage audit. Manage phase dependencies, track agent outputs, and ensure all artifacts are human-validatable with full evidence traceability.

---

## When to Use

- Starting a new session after context compression — read this + CLAUDE.md + WORKPLAN.md first
- Launching parallel agents for a phase
- Checking phase completion status
- Resolving blocked agents or failed data pulls

---

## Session Recovery Protocol

After context compression, run this sequence:
1. `Read CLAUDE.md` — recover project context, architecture, data sources
2. `Read 00-project-management/WORKPLAN.md` — check phase statuses
3. `Read 00-project-management/decisions-log.md` — recover design decisions
4. `Read {current-phase-folder}/skill.md` — recover methodology for active phase
5. Check `git log --oneline -10` — see what was committed last

---

## Agent Launch Patterns

### Parallel Launch (Independent Agents)
```
Use Task tool with 4 parallel calls:
- Agent 0A: subagent_type="general-purpose", prompt references 01-data-assembly/data-assembly.skill.md
- Agent 0B: subagent_type="general-purpose", same skill.md different section
- Agent 0C: subagent_type="general-purpose", depends on 0B output
- Agent 0D: subagent_type="general-purpose", independent
```

### Sequential Launch (Dependent Agents)
Agent 1B depends on Phase 0 outputs. Wait for all Phase 0 agents to complete before launching.

### Validation Launch (Post-Analysis)
Phase 4 agents run after Phase 3 synthesis. They read Phase 3 outputs and validate claims.

---

## Phase Completion Checklist

For each phase, verify before marking complete:
- [ ] All output files exist at expected paths
- [ ] Output files have expected row counts (no empty CSVs)
- [ ] Git commit with phase outputs
- [ ] WORKPLAN.md status updated
- [ ] Next phase dependencies are satisfied

---

## Anti-Patterns

1. **Don't launch Phase 1 before Phase 0 completes** — ingestion needs era-tagged grants
2. **Don't skip the symmetry test** — NDP-only analysis is politically vulnerable (D005)
3. **Don't commit data files to git** — use .gitignore for CSVs; commit only schemas, queries, and reports
4. **Don't modify ministry-genealogy-graph files** — that's a separate validated project; read-only
