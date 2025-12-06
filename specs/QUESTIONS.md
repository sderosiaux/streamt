# Open Questions

This file tracks all open questions that need answers before implementation.

## High-Level Questions

### Product Scope

- [ ] Q1: Quel est le scope du MVP? (Proxy only? Proxy + Flink? + Connect?)
- [ ] Q2: OSS ou propriétaire? Quel modèle de distribution?
- [ ] Q3: Quel persona principal? (Data engineer? Platform engineer? Developer?)
- [ ] Q4: Intégration Conduktor existant ou produit standalone?

### Architecture

- [ ] Q5: Monorepo centralisé ou fédération multi-repos?
- [ ] Q6: Où vit la "source of truth"? (Git only? Git + DB Conduktor?)
- [ ] Q7: Comment gérer le state des jobs Flink? (Savepoints, upgrades)

### DSL Design

- [ ] Q8: Format unique YAML ou support SQL files séparés (comme dbt)?
- [ ] Q9: Templating: Jinja comme dbt ou autre chose?
- [ ] Q10: Versioning des modèles: comment gérer v1 -> v2?

### Runtime

- [ ] Q11: Le compilateur tourne où? (CLI local? Service Conduktor?)
- [ ] Q12: Déploiement: GitOps push ou apply via CLI?
- [ ] Q13: Rollback: comment revenir en arrière?

---

## Decisions Made

| Question | Decision | Rationale | Date |
|----------|----------|-----------|------|
| Q1: Scope MVP | Proxy + Flink + Connect | Couvrir stateless, stateful, et sinks | 2025-12-06 |
| Q2: Distribution | Open source, gratuit | Adoption first, monétisation later | 2025-12-06 |
| Q3: Persona | Tous (data eng, dev, platform) | Le DSL doit être accessible à tous | 2025-12-06 |
| Q5: Repo structure | Monorepo unique pour commencer | Simplicité, fédération plus tard | 2025-12-06 |
| Q8: Format fichiers | Multi-YAML + single-file possible | Flexibilité selon préférence | 2025-12-06 |
| Q9: Templating | Jinja (comme dbt) | Familiarité, écosystème existant | 2025-12-06 |
| Q11-12: Compilation | Option C: generate + apply | Flexibilité: dry-run ou deploy | 2025-12-06 |
| Schema validation | Oui si SR configuré | Validation stricte quand possible | 2025-12-06 |
| Tests execution | Option C: continu ou ponctuel | Flexibilité, backends pluggables | 2025-12-06 |
| Tests actions | DLQ, block deploy, webhook HTTP | Couverture complète des besoins | 2025-12-06 |
| Security | Masking only (pas encryption) | Simplicité, pas de dép KMS | 2025-12-06 |
| **Standalone** | Fonctionne sans Conduktor/Proxy | OSS universel, Conduktor = extras | 2025-12-06 |
| Stateless sans Proxy | Choix dans DSL (topic réel ou virtuel) | Flexibilité selon infra disponible | 2025-12-06 |
| Masking sans Proxy | Option C: appliqué au sink | Faisable via Flink/Connect transforms | 2025-12-06 |
| Tests sans Flink | Erreur (KStreams = later) | Simplicité, éviter complexité deploy | 2025-12-06 |
| Flink config | Multiple clusters possibles, serverless OK | Flexibilité d'infra | 2025-12-06 |
| CLI name | `streamt` | stream + t (comme dbt) | 2025-12-06 |
| DLQ | Déclaré comme un model | Explicite, visible dans le DAG | 2025-12-06 |
| Multi-cluster MVP | 0 ou 1, multi later avec tags | Simplicité MVP | 2025-12-06 |
| Syntaxe from | Option C hybride (from optionnel, inféré du SQL) | Moins de config, plus d'inférence | 2025-12-06 |
| Langage | Python | Familier, écosystème data, comme dbt | 2025-12-06 |
| Secrets | .env pour MVP, vault plus tard | Simplicité first | 2025-12-06 |
| CI/CD | GitHub Action incluse | DevX complète | 2025-12-06 |
| UI | CLI + HTML statique | Comme dbt docs | 2025-12-06 |
| Governance rules | Dans stream_project.yml | Validation compile-time | 2025-12-06 |
| Secrets in generated/ | Non, placeholders ${VAR} | Résolus à l'apply en mémoire | 2025-12-06 |
| State management | manifest.json + query APIs | Comme dbt, APIs = source of truth | 2025-12-06 |
| Model versioning | Support v1/v2 parallèles | Comme dbt versions | 2025-12-06 |
| Nom projet | streamt | Confirmé | 2025-12-06 |
