# Academic delivery pack

| Material | Purpose |
| --- | --- |
| [Project report](PROJECT_REPORT.md) | Complete technical/experimental narrative with submission placeholders |
| [Architecture diagrams](ARCHITECTURE.md) | Six implementation-specific Mermaid diagrams |
| [Presentation specification](PRESENTATION.md) | Twelve slides with content, visuals, evidence, notes and timing |
| [Professor demonstration](DEMO_SCRIPT.md) | Dependency checks, safe isolated run, recorded fallback and shutdown |
| [Big Data concept map](BIG_DATA_CONCEPTS.md) | Concepts linked to concrete implementation/tests and limits |
| [Viva questions](VIVA_QUESTIONS.md) | Thirty-six concise implementation-specific answers |
| [Evidence inventory](EVIDENCE_INDEX.md) | Claim populations, authoritative artifacts and caveats |
| [Portfolio material](../portfolio/RESUME.md) | Resume bullets, descriptions and interview explanations |
| [Phase 6 validation](../../bda/PHASE6_VALIDATION.md) | Actual final-release tests and documentation checks |

Complete the report's institution/student/guide fields before submission. No
institutional approval or certification is implied. Use the evidence inventory to
check a quantitative statement before copying it into slides or a resume.

Run `python3 docs/academic/check_delivery.py` from the repository root for local link,
path, command-syntax and committed-evidence arithmetic checks. Mermaid render checks
require the separately described optional parser/browser tooling; they are recorded
in the Phase 6 validation report. No large benchmark rerun is part of this check.
