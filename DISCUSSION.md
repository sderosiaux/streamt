j'aimerai faire un "dbt" pour kafka (pour aider à faire des apps, projection de data, transformations etc)
chez Conduktor, on a tous les schémas, accès au data dans kafka, tous les clusters, et on a une abstraction de catalog justement (toutes les data des products team partageable dans la boite)
on est intégré avec Kafka Connect
on a une Proxy qui peut faire des transformations stateless (des sortes de "topic virtuels"), en SQL ou autre langage (genre Google CEL)
on va bientôt intégrer Flink pour des transformations stateful

j'aimerai creuser l'expérience à la dbt: des moyens déclaratifs de faire des apps/transformations/etc. avec la sécurité embedded, le lineage, l'auto-découverte des dépendances (à la dbt), la résolution on the fly etc. je suis pas expert dbt, donc aide moi à découvrir des possibilités que je vois pas, qui pourrait s'appliquer au data streaming

---

Tu es assis sur exactement ce qu’il faut pour faire un “dbt pour le streaming”. Je te propose:

1. Un rappel ultra synthétique de ce que dbt apporte vraiment
2. La traduction 1-to-1 de ces concepts dans ton monde Conduktor + Kafka + Proxy + Flink
3. Des idées “bonus” spécifiques au streaming que dbt ne couvre pas

---

## 1. Ce que dbt fait vraiment (en 6 briques)

En simplifiant à l’extrême:

1. **Models**

   * Des fichiers SQL qui définissent des tables / vues dans l’entrepôt.
   * Les modèles s’enchaînent via `ref()` qui construit automatiquement un DAG et l’ordre d’exécution. ([dbt Developer Hub][1])

2. **Sources**

   * Déclaration des tables “brutes” (ELT) dans du YAML.
   * On peut tester leur fraîcheur, leur qualité, et tracer la lignée depuis ces sources. ([dbt Developer Hub][2])

3. **Tests**

   * Tests déclaratifs (YAML + SQL) sur les modèles et les sources: `unique`, `not_null`, `accepted_values`, etc. ([dbt Developer Hub][3])

4. **Exposures**

   * Métadonnées pour décrire comment les modèles sont utilisés en aval: dashboards, notebooks, apps, pipelines ML. ([dbt Developer Hub][4])

5. **Docs + DAG**

   * Génération automatique d’une doc avec graph interactif, descriptions, tests, owners. ([dbt-labs.github.io][5])

6. **Semantic Layer / Metrics**

   * Définition centralisée des métriques et dimensions (MetricFlow).
   * Une API qui reçoit “give me revenue by country” et génère le SQL correct vers les modèles et tables. ([dbt Developer Hub][6])

Des gens ont commencé à appliquer tout ça au streaming en connectant dbt à des systèmes comme Materialize, Databricks DLT ou des “streaming tables” pour transformer des flux en temps réel tout en gardant le même DX dbt. ([dbt Labs][7])

---

## 2. Traduction dans ton contexte Conduktor

Tu as déjà les briques du runtime. Il te manque surtout le **langage déclaratif** et la **compilation** vers tes composants (Proxy, Connect, Flink).

Je te propose ce mapping de concepts:

### 2.1. Sources

Dans “dbt streaming by Conduktor”, une source, c’est:

* Un **topic Kafka brut** (avec son schema Avro/JSON/Protobuf)
* Un **cluster + namespace** (Confluent, Aiven, on-prem…)
* Un **connector source** (CDC Oracle, Debezium, HTTP, MQTT, etc.)
* Plus tard: des **streams non Kafka** (Kinesis, Pulsar, Rabbit, DB log, etc.)

Déclaration en YAML, version Conduktor:

```yaml
sources:
  - name: payments_events
    type: kafka_topic
    cluster: prod-eu
    topic: payments.events.v1
    schema_ref: schema_registry:payments.events.v1
    owner: team-payments
    pii_tags:
      - card_number: sensitive
      - customer_email: sensitive
    sla:
      max_lag_seconds: 60
```

Tu peux déjà remplir 80% de ça avec ton catalog actuel.

---

### 2.2. Stream models

L’équivalent des `models` dbt devient des **stream models**:

* Input: un ou plusieurs topics / sources
* Output:

  * soit un **topic dérivé** effectif (créé ou configuré)
  * soit un **“topic virtuel”** servi par ta Proxy (stateless)
  * soit un **job Flink** (stateful)
  * soit une **vue matérialisée dans un lac / entrepôt** via Connect

Une déclaration possible:

```yaml
models:
  - name: payments_clean
    materialized: virtual_topic         # via Proxy
    cluster: prod-eu
    key: payment_id
    from:
      source: payments_events
    sql: |
      SELECT
        payment_id,
        customer_id,
        amount_cents,
        currency,
        status,
        event_time
      FROM {{ source("payments_events") }}
      WHERE status IN ('AUTHORIZED', 'CAPTURED')

  - name: customer_balance_5m
    materialized: flink_stream
    cluster: prod-eu
    key: customer_id
    from:
      source: payments_clean
    sql: |
      SELECT
        customer_id,
        TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS window_start,
        SUM(amount_cents) AS total_amount_cents
      FROM {{ ref("payments_clean") }}
      GROUP BY
        customer_id,
        TUMBLE(event_time, INTERVAL '5' MINUTE)
```

Le moteur Conduktor:

* Analyse les `source()` / `ref()` pour construire le DAG
* Décide où exécuter chaque modèle: Proxy (stateless), Flink (stateful), Connect (sink)
* Configure automatiquement les artefacts nécessaires:

  * Proxy routes + règles de transformation
  * Jobs Flink + savepoints + checkpoints
  * Connectors et topics de sortie

---

### 2.3. Tests streaming

Traduction directe des tests dbt dans le temps réel:

* **Tests de structure**

  * schéma compatible, champs non null, valeurs autorisées
* **Tests de comportement**

  * pas de doublons sur une clé dans une fenêtre
  * aucun event en retard de plus de X minutes
  * ratio d’erreurs < 0,1 % sur les 5 dernières minutes
* **Tests contractuels entre modèles**

  * tout `customer_id` dans `payments_clean` doit exister dans `customers_stream`

Exemple:

```yaml
tests:
  - name: payments_clean_quality
    model: payments_clean
    type: streaming
    assertions:
      - unique_key:
          key: payment_id
          window: "15 minutes"
      - allowed_values:
          column: status
          values: ["AUTHORIZED", "CAPTURED"]
      - max_lag:
          column: event_time
          seconds: 300
      - foreign_key:
          column: customer_id
          ref_model: customers_stream
          ref_key: customer_id
          window: "1 hour"
    on_failure:
      severity: error
      actions:
        - pause_model: true
        - send_alert: "slack://#streaming-alerts"
```

Sur le runtime, ces tests deviennent:

* Des **requêtes continues** (Flink, ksql-like, Materialize-like) qui monitorent les flux
* Des **règles d’alerte** branchées sur ton système d’alerting
* Des **gates** de déploiement pour de nouvelles versions de modèles

---

### 2.4. Exposures pour des apps temps réel

Tu peux reprendre l’idée des exposures dbt, mais pour le temps réel:

* `type: application` pour un microservice qui consomme un topic
* `type: api` pour une API publique qui s’appuie sur un stream
* `type: ml_model` pour un modèle de scoring en ligne
* `type: dashboard` pour les cas où tu pousses le stream vers un lac / entrepôt

Exemple:

```yaml
exposures:
  - name: fraud_scoring_service
    type: application
    url: https://git.internal/fraud/scoring
    owner: team-fraud
    depends_on:
      - ref("customer_balance_5m")
      - ref("payments_clean")
    sla:
      max_end_to_end_latency_ms: 800
      availability: "99.9%"
```

Tu gagnes:

* Un graphe complet: `raw topics -> models -> apps / APIs`
* Des vues par équipe produit: “quels streams alimentent mon service ?”
* La capacité de dire: “si je change ce modèle, quels services sont impactés ?”

---

## 3. Sécurité “embedded” dans la définition des modèles

C’est là où Conduktor peut vraiment se différencier.

Le modèle ne décrit pas seulement la transformation, il décrit **comment la donnée peut être consommée**:

```yaml
models:
  - name: payments_clean
    ...
    security:
      classification:
        amount_cents: confidential
        card_number: highly_sensitive
      policies:
        - mask:
            column: card_number
            method: tokenization
            applied_in:
              - roles: [ "support", "analytics" ]
        - encrypt:
            column: card_number
            key: kms://prod/payments
        - allow:
            roles: [ "fraud-engine", "payments-core" ]
            purpose: "fraud_detection"
```

Le compilateur:

* Traduit ça en règles de Proxy (masquage, encryption, redaction)
* Génère les ACL / policies Conduktor que tu as déjà
* Attache ces règles partout où le modèle est consommé, y compris dans Flink

Tu obtiens un truc que dbt n’a pas: un **graph de modèles + un graph de droits** sur les flux.

---

## 4. Auto-découverte des dépendances “à la dbt”, version streaming

Tu peux reprendre exactement l’idée du `ref()`:

* Le DSL obligé à référencer un modèle ou un topic par nom logique
* Le moteur Conduktor construit un graphe des dépendances:

  * `topic brut -> modèle 1 -> modèle 2 -> app`
* Tu peux:

  * Visualiser le DAG comme dans dbt docs
  * L’utiliser pour ordonner les déploiements
  * Faire du **“impact analysis”** à chaque changement de modèle
  * Suggérer des **optimisations de topologie** (fusion de modèles, réduction des hops, etc.)

En plus, tu as une carte de tous les **consumers groups** réels. Tu peux enrichir le DAG avec les consumers runtime, pas seulement ceux déclarés dans la config. Ça dépasse ce que dbt voit dans un entrepôt.

---

## 5. “Résolution on the fly” pour agents et apps

Là tu peux aller au-delà de dbt:

1. Tu exposes un **API sémantique streaming** devant ton graph:

   * “Donne moi un flux `customer_balances` avec un retard maximum de 30s”
   * “Donne moi les transactions des 24 dernières heures enrichies avec la segmentation client”

2. L’API:

   * Cherche dans le graph sémantique si un modèle “assez proche” existe
   * Si oui, renvoie la config pour le consommer (topic, format, policy)
   * Sinon, propose de créer un nouveau modèle dérivé (Proxy ou Flink) à partir des modèles existants

Tu deviens le **runtime de requêtes data pour agents et apps** en temps réel, avec:

* Type-checking via les schémas
* Sécurité appliquée via les policies du modèle
* Découverte automatique des dépendances via le DAG

C’est très proche de l’esprit Semantic Layer de dbt, mais appliqué à de la donnée opérationnelle vivante et non à des tables mortes dans un entrepôt. ([dbt Developer Hub][6])

---

## 6. Un MVP concret “dbt streaming by Conduktor”

Tu peux viser une première version raisonnable:

1. **Un format de projet Git**

   * `stream_project.yml` (config globale)
   * `sources/` (topics + clusters)
   * `models/` (YAML + SQL ou CEL)
   * `tests/` (YAML de tests)
   * `exposures/` (apps, dashboards, services)

2. **Un compilateur**

   * Parse le projet
   * Construit le DAG
   * Résout où exécuter chaque modèle
   * Génère:

     * Config Proxy (virtual topics, redaction, encryption)
     * Jobs Flink (stateful)
     * Connectors (vers lac, entrepôt, search, vector DB)
     * Règles de sécurité Conduktor

3. **Une UX développeur**

   * CLI `cdk-stream run/test/deploy`
   * Intégration CI pour valider la config et les tests
   * UI Conduktor pour:

     * visualiser le DAG
     * voir les tests en temps réel (alertes)
     * inspecter les exposures / apps impactées

4. **Un cas d’usage phare pour raconter l’histoire**
   Par exemple:

   > “Les équipes Risk définissent leurs flux de scoring fraud en YAML/SQL dans Git. Conduktor compile ça en topics virtuels, jobs Flink et règles de masquage, avec monitoring de qualité temps réel, sans qu’elles aient à toucher aux clusters Kafka ou aux ACL.”

---

## 7. Prochaines étapes

Si tu veux, on peut faire:

* Un **exemple complet** “fraude carte bancaire” avec 5 modèles, 3 tests et 2 exposures
* Une première version de **DSL minimal** (SQL + annotations) qui cible uniquement:

  * Proxy pour stateless
  * Un runtime Flink SQL simple pour stateful

En gros: partir d’un “dbt mental model” et le casser dans ton runtime actuel, sans te retrouver à réinventer un Flink UI.

[1]: https://docs.getdbt.com/reference/dbt-jinja-functions/ref?utm_source=chatgpt.com "About ref function | dbt Developer Hub"
[2]: https://docs.getdbt.com/docs/build/sources?utm_source=chatgpt.com "Add sources to your DAG | dbt Developer Hub"
[3]: https://docs.getdbt.com/docs/build/data-tests?utm_source=chatgpt.com "Add data tests to your DAG | dbt Developer Hub"
[4]: https://docs.getdbt.com/docs/build/exposures?utm_source=chatgpt.com "Add Exposures to your DAG | dbt Developer Hub"
[5]: https://dbt-labs.github.io/dbt-project-evaluator/0.8/rules/documentation/?utm_source=chatgpt.com "Documentation - dbt_project_evaluator"
[6]: https://docs.getdbt.com/docs/use-dbt-semantic-layer/dbt-sl?utm_source=chatgpt.com "dbt Semantic Layer | dbt Developer Hub - dbt Labs"
[7]: https://www.getdbt.com/blog/batch-to-streaming-in-one-easy-step?utm_source=chatgpt.com "Batch to Streaming in One Easy Step"

---

"Décide où exécuter chaque modèle: Proxy (stateless), Flink (stateful), Connect (sink)"
- montre moi un exemple pour "sink"

Tu peux reprendre l’idée des exposures dbt, mais pour le temps réel:
- donne moi le use-case. je déclare des exposures sur des applications ou d'autre système (des sinks), voir même des streams existants. et alors, ou est la valeur de ce produit "dbt for streaming" ici? juste pour du lineage??

- donne moi un exemple complet oui

---

Parfait, on rentre dans le dur.

Je vais faire en 3 parties:

1. Un exemple concret de **modèle "sink"**
2. À quoi servent vraiment les **exposures** en streaming (bien plus que du lineage)
3. Un **exemple complet de projet** pour un cas “fraude carte bancaire”

---

## 1. Exemple de modèle “sink” via Kafka Connect

Contexte: tu as déjà un modèle streaming `fraud_events_enriched` qui écrit dans un topic Kafka.
Tu veux **déclarer dans le projet** que ce modèle doit être poussé vers Snowflake via un Connect sink.

```yaml
models:
  - name: fraud_events_enriched
    materialized: flink_stream
    cluster: prod-eu
    key: payment_id
    from:
      source: payments_clean
    sql: |
      SELECT
        payment_id,
        customer_id,
        risk_score,
        event_time
      FROM {{ ref("payments_clean") }} p
      JOIN {{ ref("customer_risk_profile") }} c
      USING (customer_id)

  - name: fraud_events_warehouse
    materialized: sink
    from:
      model: fraud_events_enriched
    sink:
      type: snowflake_table         # "cible logique"
      connector: snowflake-sink     # nom logique du connector type
      cluster: prod-eu
      topic: fraud.events.enriched
      config:                       # traduit en config Kafka Connect
        snowflake_database: FRAUD
        snowflake_schema: PROD
        snowflake_table: FRAUD_EVENTS_ENRICHED
        key_mode: RECORD_KEY
        key_columns: ["payment_id"]
        insert_mode: UPSERT
        flush_interval_sec: 30
        max_retries: 5
```

Le moteur “dbt streaming” Conduktor ferait:

* Vérifie que `fraud_events_enriched` existe et est compatible avec le schéma attendu
* Génère ou met à jour la **config du Connect sink** correspondant
* Suit la santé de ce sink comme partie du modèle (`status`, lag, erreurs)
* Intègre ce sink dans le **DAG global**:
  `payments_raw -> payments_clean -> customer_risk_profile -> fraud_events_enriched -> fraud_events_warehouse`

Tu as donc un modèle logique “sink” dans le même graphe que tes modèles Proxy / Flink.

---

## 2. Exposures en streaming: où est la valeur, concrètement ?

Tu déclares des exposures sur:

* des **apps** (microservices, scoring service)
* des **systèmes aval** (Snowflake, ClickHouse, Elastic, dashboards BI)
* des **streams existants** déjà utilisés par d’autres équipes

Ce n’est pas seulement du lineage décoratif.

Voici ce que ça permet avec un moteur comme Conduktor derrière.

### 2.1. Contrats d’usage, pas seulement de la lignée

Un exposure devient un **contrat**:

* qui utilise la donnée
* pour quel usage
* avec quels SLO (fraîcheur, latence, taux d’erreur)
* avec quelles contraintes de sécurité

Exemple:

```yaml
exposures:
  - name: fraud_scoring_service
    type: application
    owner: team-fraud
    url: https://git.internal/fraud/scoring
    depends_on:
      - ref("fraud_events_enriched")
    sla:
      max_end_to_end_latency_ms: 800
      max_error_rate: 0.001
    access:
      allowed_roles: ["fraud-engine", "fraud-ml"]
      purpose: "fraud_detection"
```

Valeur:

* Chaque modèle sait **qui dépend de lui** et avec quelle attente
* Tu peux bloquer ou mettre en quarantaine un déploiement qui **casse un SLO d’exposure**
* Tu peux générer une **doc orientée “produits et apps”**, pas juste une doc de tables / topics

### 2.2. Impact analysis pour les changes

Quand quelqu’un veut changer un modèle ou un schéma:

* Le moteur va chercher tous les `exposures.depends_on` impactés
* Il liste les apps / dashboards qui risquent de casser
* Il propose des stratégies de changement:

  * nouvelle version de modèle + période de double écriture
  * migration progressive des consumers
  * garde-fous sur les tests avant de couper l’ancien chemin

Tu aides les teams à faire des changements dans Kafka **sans casser des apps en prod**.
C’est déjà une vraie douleur dans les banques.

### 2.3. Gouvernance et sécurité orientées usage

Les exposures te donnent un angle “usage réel” pour la sécurité.

Par exemple:

* Un exposure “Customer 360 Dashboard” doit **exclure certaines colonnes** pour le support
* Tu traduis ça en **policies Proxy et ACL** automatiquement sur les topics et les sinks
* Tu peux prouver à un auditeur:

  * quelles apps ont accès à quelles catégories de données
  * via quels flux
  * sous quels masquages / encryptions

C’est un pont direct entre **GRC** et **infrastructure streaming**, sans Excel.

### 2.4. Expérience self-service

Pour un product team, la doc “exposures” devient la **carte des produits de données**:

* “Je suis l’équipe Onboarding, quels streams ou tables je peux utiliser pour voir tout le lifecycle client, en temps réel ou quasi temps réel ?”
* Elle tombe sur des exposures comme `customer_360_api`, `fraud_events_dashboard`, `kyc_warehouse_view`
* Elle clique, voit de quoi ça dépend, quel est le schéma, qui est owner, comment demander l’accès.

Ton produit “dbt for streaming” devient l’entrée de gamme pour:

* découvrir des flux utiles
* demander l’accès
* comprendre les impacts d’un nouveau besoin

---

## 3. Exemple complet: parcours fraude de bout en bout

Je te fais un mini projet “dbt streaming Conduktor” avec:

* des **sources** (topics bruts, CDC DB)
* des **modèles** Proxy, Flink, Sink
* des **exposures** sur app et dashboard

### 3.1. Sources

```yaml
sources:
  - name: card_payments_raw
    type: kafka_topic
    cluster: prod-eu
    topic: payments.raw.v1
    schema_ref: sr://payments.raw.v1
    owner: team-payments
    pii_tags:
      card_number: highly_sensitive
      customer_email: sensitive

  - name: customers_cdc
    type: kafka_topic
    cluster: prod-eu
    topic: crm.customers.cdc
    schema_ref: sr://crm.customers.v3
    owner: team-crm
    pii_tags:
      customer_email: sensitive
      national_id: highly_sensitive
```

### 3.2. Modèles streaming

#### 1) Normalisation stateless via Proxy

```yaml
models:
  - name: card_payments_clean
    materialized: virtual_topic           # Proxy Conduktor
    cluster: prod-eu
    key: payment_id
    from:
      source: card_payments_raw
    sql: |
      SELECT
        payment_id,
        customer_id,
        amount_cents,
        currency,
        status,
        event_time,
        card_network
      FROM {{ source("card_payments_raw") }}
      WHERE status IN ('AUTHORIZED', 'CAPTURED')
    security:
      classification:
        amount_cents: confidential
        card_network: internal
      policies:
        - mask:
            column: card_number
            method: tokenization
            applied_in:
              - roles: ["support", "analytics"]
        - encrypt:
            column: card_number
            key: kms://prod/payments
```

Ce modèle sera traduit en:

* Un **topic virtuel** servi par Proxy
* Des règles de masquage / encryption dans Proxy et dans tes policies Conduktor

#### 2) Enrichissement stateful via Flink

```yaml
  - name: customer_risk_profile
    materialized: flink_stream
    cluster: prod-eu
    key: customer_id
    from:
      source: customers_cdc
    sql: |
      SELECT
        customer_id,
        risk_band,
        kyc_status,
        updated_at
      FROM {{ source("customers_cdc") }}
      WHERE kyc_status IN ('APPROVED', 'REVIEW')

  - name: card_payments_fraud_events
    materialized: flink_stream
    cluster: prod-eu
    key: payment_id
    from:
      sources:
        - card_payments_clean
        - customer_risk_profile
    sql: |
      SELECT
        p.payment_id,
        p.customer_id,
        p.amount_cents,
        p.currency,
        p.card_network,
        p.event_time,
        c.risk_band,
        c.kyc_status,
        FRAUD_SCORE(p.*, c.*) AS risk_score,
        CASE
          WHEN FRAUD_SCORE(p.*, c.*) > 0.9 THEN 'BLOCK'
          WHEN FRAUD_SCORE(p.*, c.*) > 0.7 THEN 'REVIEW'
          ELSE 'ALLOW'
        END AS decision
      FROM {{ ref("card_payments_clean") }} p
      JOIN {{ ref("customer_risk_profile") }} c
        ON p.customer_id = c.customer_id
        AND p.event_time BETWEEN c.updated_at - INTERVAL '30' DAY AND c.updated_at
```

Ce modèle devient un **job Flink** géré par Conduktor:

* avec state, checkpoints, savepoints
* branché automatiquement sur les topics d’entrée et de sortie

#### 3) Modèle “sink” vers entrepôt

```yaml
  - name: fraud_events_warehouse
    materialized: sink
    from:
      model: card_payments_fraud_events
    sink:
      type: snowflake_table
      connector: snowflake-sink
      cluster: prod-eu
      topic: fraud.events.enriched
      config:
        snowflake_database: FRAUD
        snowflake_schema: PROD
        snowflake_table: FRAUD_EVENTS_ENRICHED
        insert_mode: UPSERT
        key_columns: ["payment_id"]
        flush_interval_sec: 30
```

Le compilateur:

* Génère la config du connector Snowflake
* S’assure que le schéma de `card_payments_fraud_events` et celui de la table sont compatibles
* Ajoute des tests de fraîcheur sur la table, si tu les définis

### 3.3. Tests streaming

```yaml
tests:
  - name: fraud_events_quality
    model: card_payments_fraud_events
    type: streaming
    assertions:
      - max_latency:
          source: card_payments_raw
          target: card_payments_fraud_events
          max_ms: 500
      - allowed_values:
          column: decision
          values: ["ALLOW", "BLOCK", "REVIEW"]
      - distribution:
          column: risk_score
          buckets:
            - { min: 0.0, max: 0.3, max_ratio: 0.7 }
            - { min: 0.3, max: 0.7, max_ratio: 0.25 }
            - { min: 0.7, max: 1.0, max_ratio: 0.1 }
    on_failure:
      severity: error
      actions:
        - pause_model: true
        - send_alert: "slack://#fraud-alerts"
```

Ces tests tournent en continu et peuvent:

* pauser le job
* signaler des dérives (drift)
* bloquer une nouvelle version de modèle qui dégrade les distributions

### 3.4. Exposures

Enfin, tu exposes ce que les humains utilisent vraiment.

```yaml
exposures:
  - name: fraud_scoring_service
    type: application
    owner: team-fraud
    url: https://git.internal/fraud/scoring
    depends_on:
      - ref("card_payments_fraud_events")
    sla:
      max_end_to_end_latency_ms: 800
      max_error_rate: 0.001
    access:
      allowed_roles: ["fraud-engine", "fraud-ml"]
      purpose: "fraud_detection"

  - name: fraud_analyst_dashboard
    type: dashboard
    tool: tableau
    url: https://tableau.internal/fraud/dashboard
    owner: team-fraud-analytics
    depends_on:
      - ref("fraud_events_warehouse")
    freshness:
      max_lag_minutes: 5
    access:
      allowed_roles: ["fraud-analytics", "risk-management"]
      purpose: "reporting_and_investigation"
```

Valeur concrète ici:

* Tu vois tout le chemin `card_payments_raw -> ... -> card_payments_fraud_events -> fraud_events_warehouse -> fraud_analyst_dashboard`
* Quand tu changes `card_payments_clean`, le système affiche:

  * “Tu impactes: `fraud_scoring_service`, `fraud_analyst_dashboard`”
  * “Les SLO suivants risquent de ne plus être respectés”
* Les équipes GRC peuvent naviguer par **usage**:

  * “Montre moi toutes les apps qui utilisent des données `highly_sensitive`”
  * “Montre moi toutes les exposures liées à l’usage ‘fraud_detection’”

---

Si tu veux, prochaine étape on peut:

* découper ça en un **mini design de produit**:
  CLI, structure de repo, compilation vers Proxy / Flink / Connect
* choisir un seul cas d’usage phare (fraude, KYC, Customer 360) et le packager comme “dbt streaming starter kit” pour le montrer en démo.

---

Oui, ta solution “dbt sur le streaming” adresse une grosse partie de ce qui est dit.
Pas tout (il y a beaucoup d’organisationnel), mais tu as de quoi te positionner très clairement.

Je te fais un tour thème par thème, avec à chaque fois:

* comment ça mappe à ton “dbt streaming”
* si c’est couvert, partiellement ou pas

---

## 1. Gouvernance

**Ce qu’ils disent**

* Rôles, RBAC strict, segmentation sensible / non sensible
* Data residency, pays, AI Act, usage approprié
* Risque d’export vers Excel / systèmes hors contrôle
* “C’est plus des problèmes de prise de conscience, d’usage accidentel, etc.”

**Ce que ton “dbt streaming” fait bien**

Avec:

* **Classification dans les modèles**

  * Dans la config du modèle / source: `classification`, `pii_tags`, `purpose`, `allowed_roles`.
  * Tu forces les équipes à dire “ce champ est sensible, celui-ci non”.
  * Sans classification, impossible de déployer.

* **Sécurité “dans le modèle” + Proxy / Gateway**

  * `mask / encrypt / allow` dans la définition du modèle → compilé en:

    * règles Proxy (masking, tokenisation, encryption)
    * policies Conduktor (RBAC, autorisations par rôle / team)
  * Tu appliques ça sur les streams **et** sur les sinks.

* **Data residency / zones**

  * Tu peux ajouter dans les modèles:

    * `region: EU` / `region: US`
    * `allowed_clusters` / `forbidden_sinks`
  * Le compilateur interdit un sink Snowflake US pour un modèle marqué `region: EU` par exemple.

* **AI / training data exposures**

  * Tu peux déclarer des exposures `type: ml_training` avec:

    * use-case, niveau de risque, besoin d’audit
  * Tu tiens la liste des datasets de training et leur lignée.
  * Tu peux ajouter des tests spécifiques: bias, représentativité, etc.

**Ce qui est partiel**

* La partie “awareness”, culture, formation → la plateforme aide, ne remplace pas.
* Tu peux imposer la classification et les controls, mais quelqu’un doit quand même décider.
* L’EU AI Act, RGPD etc. demandent une interprétation juridique que tu ne fournis pas, tu fournis la **boîte à outils** pour appliquer les décisions.

Je dirais: **gouvernance technique très bien couverte**, gouvernance “humaine / politique” supportée mais pas résolue.

---

## 2. Data Quality & contrats

**Ce qu’ils disent**

* Enforcer les contrats, éviter les changements de schéma sauvages
* Contrôles à chaque hop, automatisés
* Data drift, monitoring en continu
* 10–15 hops différents, techno / teams différentes
* “Handshake” entre équipes sur les contrôles
* Rejeter ou tagger la mauvaise qualité et renvoyer aux bonne équipes

**Ce que ton “dbt streaming” couvre bien**

* **Contrats explicites via modèles**

  * Chaque modèle définit: schéma, types, clés, sémantique.
  * Tu peux gérer des versions (`fraud_events_v1`, `v2`) et imposer compatibilité.

* **Tests streaming à chaque hop**

  * Tests déclaratifs: `max_latency`, `not_null`, `allowed_values`, `distribution`, `foreign_key`, etc.
  * Exécutés en continu (Flink / moteur interne) avec actions: `pause_model`, `alert`, `tag_bad_data`.

* **“Handshake” codé dans Git**

  * Tu peux imposer qu’un changement de modèle contractuel:

    * nécessite validation des owners des exposures impactés
    * passe des tests de qualité + compatibilité avant déploiement
  * Ce “handshake” devient un process dans CI/CD, pas juste un mail.

* **Multi-hops hétérogènes**

  * Tant que les hops passent par des éléments gérés par Conduktor (topics, Proxy, Flink, Connect sinks, etc.), tu peux tester et tracer.
  * Tu donnes une vue bout en bout “11 hops” sur le graphe.

**Ce qui est partiel**

* Si certains hops sont hors de ta portée (scripts ad hoc, Excel, ETL historique), tu ne vois pas tout.
* Tu peux offrir des connecteurs / hooks pour capturer des métriques mais tu ne contrôles pas tout l’écosystème.

Globalement: **data quality technique et contrats très bien couverts**, à condition que la majorité des hops passent par ta plateforme.

---

## 3. Complexité des workflows & multi-hops

**Ce qu’ils disent**

* 10–15 hops avant la lakehouse
* Outils, technologies, équipes, connaissances différentes
* Connaissance perdue, “on sait que ça existe, mais plus personne ne maintient”
* Vrai sujet: ownership, contrats entre chaque hop, lignée complète
* Business vs tech vs sécurité, intérêts différents

**Ce que ton “dbt streaming” apporte**

* **DAG centralisé et versionné**

  * Tout est décrit dans un projet Git: sources, modèles, tests, exposures.
  * Le graph complet “hop 1 à hop 15” est calculé, visible et versionné.
  * Tu évites le “on sait que ça existe mais plus personne ne sait comment”.

* **Ownership explicite**

  * Chaque modèle / exposure a des owners (`team-payments`, `team-fraud`, etc.).
  * Chacun voit “ce que je produis”, “ce que j’utilise”, “ce qui dépend de moi”.

* **Contrats entre hops**

  * Chaque hop est un modèle avec: schéma, tests, SLO, sécurité.
  * Tu visualises les contrats brisés et tu génères des warnings / blocages.

* **Vue par domaine et par use-case**

  * Les exposures te donnent une vue “business”:

    * “Tout ce qui alimente le use-case `fraud_scoring`”
    * “Tous les flux pour `regulatory_reporting`”

**Ce qui reste hors périmètre**

* Les problèmes de politique interne, budgets, priorités entre lignes métier.
* Les guerres de modèles conceptuels (“on ne s’accorde pas sur la définition de ‘customer’”).

  * Tu fournis un cadre pour les mettre d’accord en YAML, tu ne crées pas l’accord.

C’est **très bien aligné avec leurs douleurs de complexité et de connaissance perdue**.

---

## 4. Realtime & valeur du temps

**Ce qu’ils disent**

* Retards = perte d’avantage compétitif
* Données temps réel ou presque, surtout pour risque, marché, crédit
* Minimiser les retards entre source, traitement, lakehouse, usage final

**Ce que ton “dbt streaming” fait bien**

* Les modèles sont **nativement streaming**, pas batch:

  * Proxy → virtual topics temps réel
  * Flink → stateful avec latence maîtrisée
  * Sinks → vers lakehouse en quasi temps réel

* Tu peux définir des **SLO de latence** dans les tests et les exposures:

  * `max_latency` entre topic source et modèle
  * `max_end_to_end_latency_ms` pour un exposure (app, dashboard)

* Tu peux mettre en place:

  * des alertes si la latence explose
  * des dégradations planifiées (par ex. désactiver certains enrichissements coûteux pour tenir la latence)

Ici, tu es clairement en plein dans leur sujet: **tenir la promesse temps réel avec contrôle**.

---

## 5. Temps pour créer des pipelines

**Ce qu’ils disent**

* Des centaines de pipelines, cycles de “weeks to months”
* Là où ça prend du temps:

  * trouver les bonnes sources
  * se mettre d’accord sur le mapping et le modèle cible
  * mettre en place DQ, classification, governance
* Beaucoup de temps en pré-développement

**Ce que ton “dbt streaming” accélère**

* **Réutilisation et standardisation**

  * Sources déclarées une fois, réutilisées partout.
  * Modèles typiques packagés en templates: “CDC Oracle → stream clean”, “Payments → Fraud Features”, etc.

* **Démarrage rapide pour nouvelles équipes**

  * Catalogue Conduktor + projets “dbt-like” =

    * “Voici les sources autoritatives”
    * “Voici les flux de référence déjà validés et testés”

* **DQ et sécurité par design**

  * Tu fournis des “blueprints” de tests et de policies.
  * Les équipes ne repartent pas de zéro à chaque pipeline.

**Ce qui reste long**

* Identifier les bons attributs, négocier le modèle commun, impliquer les SME.
* Ton outil rend ça plus visible et documenté, mais ne remplace pas les réunions.

Donc: **tu réduis fortement la partie répétitive**, tu n’effaces pas la complexité métier.

---

## 6. Risque & “mieux pas de data que mauvaise data”

**Ce qu’ils disent**

* Mieux vaut pas de donnée que de la donnée incorrecte
* Classification comme base de tout contrôle
* Zero trust, audit de chaque accès et mouvement
* “C’est plus facile à dire qu’à faire”

**Ce que ton “dbt streaming” propose**

* **Fail-closed par défaut**

  * Sans classification ou tests minimum, pas de déploiement de modèle.
  * En cas de DQ catastrophique:

    * pause du modèle
    * blocage des sinks
    * alertes aux owners

* **Classification obligatoire pour les modèles**

  * Tu obliges à attribuer une classe à chaque champ important.
  * Tu peux brancher de la détection automatique PII pour suggérer des classes.

* **Zero trust intégré**

  * Chaque modèle = un point de contrôle:

    * qui peut consommer
    * pour quel usage (purpose)
    * avec quels masquages / encryptions
  * Tu compiles ça en policies Gateway / Proxy / ACL sur Kafka.

* **Audit unifié**

  * En théorie, tu peux lier:

    * “qui a lu ce modèle / ce sink”
    * “quelles policies étaient appliquées”
    * “dans quel contexte (app / exposure)”

**Ce qui reste difficile**

* La partie “globale” zero trust (IdP, SIEM, corrélation d’évènements) dépasse ta seule plateforme, même si tu t’y connectes.
* Les arbitrages business “on prend ce risque ou pas” restent humains.

---

## Résumé brutal

* **Oui, ton “dbt pour le streaming” colle étonnamment bien à 80 % des douleurs exprimées**

  * contrats entre hops
  * qualité gérée partout
  * visibilité bout en bout
  * sécurité intégrée
  * temps réel maîtrisé

* **Là où c’est partiel ou externe**

  * alignement politique entre lignes métier
  * choix de modèles communs, définitions métier
  * interprétation fine de la loi (AI Act, RGPD, etc.)
  * outils hors de ta portée

Tu peux sans problème construire ton pitch autour de ça:

> “Vous avez décrit vos problèmes en termes d’hops, de contrats, de gouvernance, de délai et de risque.
> Notre idée ‘dbt sur le streaming’ est une couche commune, déclarative, qui rend ces choses:
> 1/ visibles,
> 2/ testables,
> 3/ enforceables,
> sur tous vos flux Kafka, vos jobs de transformation et vos sinks vers lakehouses et AI.”

Si tu veux, on peut maintenant prendre **3 citations très fortes** et écrire la slide “Ce que vous avez dit” / “Ce que notre approche change concrètement” pour ton prochain webinar ou deck.

---

Très bonne question, c’est là que tout devient intéressant.

Tu as deux réalités dans le streaming:

* des **apps qui PRODUISENT** (producers Java/Python, CDC, etc.)
* des **apps qui CONSOMMENT** (microservices, services de scoring, APIs, jobs batch, etc.)

Ton “dbt for streaming” doit représenter les deux, sinon le graph reste incomplet.

Je te propose:

1. Oui, on les représente comme des **exposures**, mais enrichies
2. On distingue bien le rôle: **producer** vs **consumer**
3. On relie ça aux topics / modèles pour avoir un vrai end-to-end

---

## 1. Concept simple

Tu peux garder ce principe:

* Le **coeur** de ton projet:

  * `sources` = ce que produit le monde applicatif
  * `models` = transformations Proxy / Flink / sinks

* Les **apps** (Java, Python, Spark, etc.) sont des **exposures de type "application"** avec un rôle:

  * `role: producer`
  * `role: consumer`
  * ou `role: producer_consumer` si l’app fait les deux

Du coup ton graphe ressemble à ça:

`App producteur -> source(topic) -> models(streaming) -> sinks -> App consommateur / dashboard`

Et tout est visible dans un seul DAG.

---

## 2. App producteur comme exposure

Exemple: un service `payments-api` en Java qui publie sur `payments.raw.v1`.

```yaml
exposures:
  - name: payments_api
    type: application
    role: producer
    owner: team-payments
    language: java
    repo: https://git.internal/payments/api
    produces:
      - source("card_payments_raw")
    sla:
      max_produce_latency_ms: 50
      availability: "99.9%"
    contracts:
      schema: strict          # refus des events qui ne match pas le schema
      versioning: "backward"  # contrainte de compatibilité
```

Ce que tu peux en faire côté plateforme:

* Tu sais **qui est responsable** de la source `card_payments_raw`
* Quand quelqu’un veut changer ce schema, tu sais quel repo et quelle équipe sont concernées
* Tu peux brancher:

  * des métriques runtime (taux d'erreurs de production, invalid events)
  * une policy “pas de nouveau champ sans migration plan”

Tu lies enfin le monde “Kafka” au monde “Java/Python” de façon explicite.

---

## 3. App consommateur comme exposure

Exemple: un service `fraud_scoring_service` en Python qui consomme `card_payments_fraud_events`.

```yaml
exposures:
  - name: fraud_scoring_service
    type: application
    role: consumer
    owner: team-fraud
    language: python
    repo: https://git.internal/fraud/scoring
    consumes:
      - ref("card_payments_fraud_events")
    sla:
      max_end_to_end_latency_ms: 800
      max_error_rate: 0.001
    access:
      allowed_roles: ["fraud-engine", "fraud-ml"]
      purpose: "fraud_detection"
    consumer_config:
      consumer_group: fraud-scoring-v3
      max_lag_messages: 10000
```

Ce que tu peux en faire:

* Impact analysis direct:

  * si tu touches `card_payments_fraud_events`, tu vois `fraud_scoring_service` dans la liste des impactés
* Tu peux surveiller:

  * lag par consumer group
  * respect des SLO
* Tu peux brancher des vérifications de compatibilité:

  * schema du modèle vs ce que l’app consomme réellement (via schema registry ou payloads observés)

---

## 4. End-to-end avec producers ET consumers

Reprenons le cas complet “fraude” (simplifié):

* `payments_api` (producer app Java)
  → produit `card_payments_raw` (`source`)
* `card_payments_clean` (`model` Proxy)
* `card_payments_fraud_events` (`model` Flink)
* `fraud_events_warehouse` (`model` sink vers Snowflake)
* `fraud_scoring_service` (consumer app Python)
* `fraud_analyst_dashboard` (exposure de type dashboard)

Ton DAG logique:

`payments_api (exposure producer)
    -> source("card_payments_raw")
    -> ref("card_payments_clean")
    -> ref("card_payments_fraud_events")
    -> ref("fraud_events_warehouse")
    -> fraud_scoring_service (exposure consumer)
    -> fraud_analyst_dashboard (exposure dashboard)`

Ce graph montre enfin **tout**:

* qui écrit en premier
* comment la donnée est nettoyée / enrichie
* où elle sort vers la lakehouse
* qui la consomme, en temps réel et en analytique

---

## 5. Pourquoi ça vaut le coup de modéliser les apps comme ça

Quelques bénéfices très concrets:

1. **Ownership clair de bout en bout**

   * `source` = app producteur identifiée
   * `model` = équipe de données / plateforme
   * `exposure` = équipe produit / métier

2. **Change management structuré**

   * Tu veux changer une clé ou une colonne dans un modèle ?
   * Tu vois tout de suite quels producers et quels consumers sont impactés
   * Tu peux imposer:

     * une migration sur le repo `payments_api`
     * une validation par `team-fraud` avant déploiement

3. **Governance et sécurité alignées sur la réalité applicative**

   * Tes policies ne s’appliquent pas “à un topic abstrait”, mais à

     * “le topic que `payments_api` produit”
     * “le flux que `fraud_scoring_service` lit”

4. **Outil de dialogue entre équipes**

   * Quand une équipe métier dit “mon app de scoring est cassée”, tu peux naviguer:

     * de l’exposure `fraud_scoring_service`
     * vers les modèles
     * vers la source `payments_api`
   * Ça matérialise ce qu’ils décrivaient dans le webinar: “qui possède quoi, quels contrats entre chaque hop”.

---

## 6. Design simple pour ton DSL

Donc ma réponse courte à ta question:

> “comment on les représente ici? des exposures?”

Oui, tu peux:

* Garder un seul objet `exposures`
* Ajouter un champ `role: producer | consumer | dashboard | ml_training | other`
* Ajouter `produces` et `consumes` pour relier aux `sources` / `models`

Si tu veux, je peux te proposer un mini schéma formel du DSL:

* `sources`
* `models`
* `tests`
* `exposures` (avec rôle producer/consumer/etc.)

plus une page “vue graph” avec les apps en bout de chaîne.

---

Bonne question de cadrage.

Dans notre “dbt for streaming”, on garde la même logique que dbt classique:

### 1. `source("card_payments_raw")`

* Ça pointe vers **une source déclarée dans la section `sources:`** de ton projet.
* Cette source représente de la donnée **produite par un système externe** au projet de transformations:

  * une app Java qui produit dans un topic
  * un CDC Oracle → Kafka
  * un flux qui vient d’un autre domaine / équipe
* Tu **ne contrôles pas** la logique qui écrit dedans, tu la décris:

  * où ça vit (cluster, topic)
  * quel schéma
  * qui est owner
  * quels tags de sensibilité, SLA, etc.

Donc: `source()` = “prends cette donnée à l’entrée du graph, telle qu’elle arrive du monde extérieur”.

> Ce n’est pas “non déclaré” dans le projet, au contraire.
> C’est **déclaré dans `sources:`**, mais considéré comme **externe** au moteur de modèles.

---

### 2. `ref("card_payments_fraud_events")`

* Ça pointe vers **un modèle défini dans la section `models:`** de ton projet.
* Ce modèle est une **transformation que ton projet possède**:

  * Proxy (stateless)
  * Flink (stateful)
  * Sink vers Snowflake / autre
* Le compilateur sait:

  * comment exécuter ce modèle
  * sur quel topic / job / connector le déployer
  * dans quel ordre le lancer par rapport aux autres modèles

Donc: `ref()` = “utilise la sortie d’un autre modèle géré par mon projet”.

---

### 3. Résumé simple

* `source("card_payments_raw")`

  * Déclaré dans `sources:`
  * Produit par une appli / système externe
  * Point d’entrée du graph

* `ref("card_payments_fraud_events")`

  * Déclaré dans `models:`
  * Produit par un modèle de ton projet (Proxy, Flink, sink, etc.)
  * Nœud interne du graph, entièrement contrôlé par Conduktor

En phrase courte:

> `source()` = je me branche à un flux existant.
> `ref()` = je me branche à un autre modèle que mon projet fait tourner.

---

Je vais séparer en deux niveaux:

1. **Comment dbt gère vraiment Git et les projets aujourd’hui**
2. **Comment tu peux copier ça en mieux pour ton “dbt for streaming” avec fédération + plateforme centrale**

---

## 1. Comment dbt gère projets + Git

### 1.1. Un projet dbt = un répertoire Git

Dans dbt, un **projet** c’est juste un dossier avec:

* `dbt_project.yml`
* des dossiers `models/`, `tests/`, `snapshots/`, `macros/`, etc. ([dbt Developer Hub][1])

Ce dossier est versionné dans **un repo Git**. Que tu sois en dbt Core ou dbt Cloud, tu pointes toujours sur un repo Git comme source de vérité. ([dbt Developer Hub][2])

Donc:

* Les **models**, **sources**, **exposures**, **tests**, tout ça = des fichiers dans ce repo
* Git sert à:

  * faire des branches
  * faire des PR avec review
  * lancer du CI (tests dbt) avant merge

### 1.2. Monorepo vs multi repo côté dbt

dbt supporte plusieurs architectures:

* **Un seul projet centralisé**

  * Un `dbt_project.yml`
  * Tous les modèles pour toute l’orga dans ce projet
  * Simple au début, tout est visible au même endroit ([phData][3])

* **Architecture multi-projets (dbt Mesh)**

  * Plusieurs projets dbt, chacun avec son `dbt_project.yml`
  * Chaque projet peut être dans:

    * un **monorepo** (plusieurs projets dans un même repo)
    * ou un **multi-repo** (un repo Git par projet) ([dbt Developer Hub][4])
  * En pratique, modèle fréquent: 1 projet dbt = 1 domaine = 1 équipe

Pour partager entre projets, dbt recommande:

* de déclarer certains modèles comme “publiques” / stables
* de les exposer via des **packages** ou dépendances entre projets (le projet B déclare A comme dépendance, puis peut réutiliser ses modèles comme source contractuelle) ([Infostrux Engineering Blog][5])

Donc: **fédération supportable** oui, mais chaque projet reste une unité Git et de compilation indépendante.

---

## 2. Comment traduire ça pour ton “dbt for streaming”

Tu peux copier cette logique en la poussant plus loin pour Kafka.

### 2.1. Deux couches claires

1. **Repos d’équipes (fédérés)**

   * Chaque team a son repo `streaming-project-<domaine>`
   * Dans ce repo:

     * `stream_project.yml`
     * `sources/`, `models/`, `tests/`, `exposures/`
   * Ils décrivent:

     * les topics qu’ils produisent (via `source()`)
     * les modèles de transformation (Proxy, Flink, sinks)
     * leurs apps productrices / consommatrices (exposures)

2. **Service “plateforme streaming” Conduktor**

   * Ce service:

     * récupère les projets depuis Git (via API ou webhooks)
     * compile chaque projet
     * construit un **graph global** de tous les sources/models/exposures
     * applique des **règles globales** (classification obligatoire, tests minimum, conventions, etc.)
     * expose une UI unique pour voir tous les tests, tous les modèles, tous les flots

Tu obtiens le même confort qu’un gros monorepo, tout en gardant des repos par équipe.

### 2.2. Résolution des dépendances entre repos

Deux façons simples de faire:

#### Option A – Tout ce qui est cross-team passe par des `source()` Kafka

Dans Kafka, la surface partagée entre équipes, c’est un **topic**.

* L’équipe A publie sur `payments.raw.v1`

  * Elle déclare `source("payments_raw")` dans *son* projet
* L’équipe B consomme `payments.raw.v1`

  * Elle déclare **le même topic** comme `source("payments_raw_paymentsdomain")` dans *son* projet
  * ou comme `source("payments_raw_from_teamA")`

Le lien cross-repo ne vient pas de `ref()`, il vient de:

* la **même réalité physique** (topic, cluster, schéma)
* le moteur Conduktor qui reconnaît: “ce topic est produit par tel projet, consommé par tel autre”

Concrètement:

* Pour le **compilateur** de chaque projet: il voit des `source()`, point.
* Pour la **plateforme**, tu reconstruis un graphe inter-projets à partir des métadonnées:

  * qui produit ce topic
  * qui le consomme
  * quels tests sont alignés ou pas

Ça reste très proche de dbt Mesh: chaque domaine publie des “data products” sur un médium commun, les autres s’y connectent avec un contrat. ([dbt Developer Hub][4])

#### Option B – Supporter un `ref("team.model")` cross-projet

Si tu veux un truc plus “dbt pur”:

* Chaque projet peut marquer certains modèles comme **publics**:

  * `public: true`
  * `contract: {schema, compatibilité, SLO}`

* Un autre projet peut écrire:

  * `ref("team_payments.card_payments_clean")`

Le service Conduktor:

* connaît la liste de tous les projets, leurs modèles, leurs noms logiques
* vérifie que `team_payments.card_payments_clean` existe et est public
* sait que ce modèle correspond à tel topic / telle transformation

Ça demande un peu plus de travail sur la résolution de noms, par contre ça te rapproche du mode mental dbt.

### 2.3. Rôle de l’équipe plateforme dans ce modèle

L’équipe plateforme peut avoir son propre repo:

* **Repo “platform-streaming-rules”**

  * contient:

    * macros / templates de tests
    * règles globales: naming, classification, sécurité minimum
    * policies par défaut (zero trust, logs, etc.)

Et la plateforme:

* impose que chaque projet:

  * inclue ce “package” plateforme
  * respecte un certain niveau de tests et de classification
  * utilise certains macros pour la sécurité (mask/encrypt/etc.)

* agrège:

  * tous les artefacts de compilation
  * tous les résultats de tests
  * toutes les exposures

Dans l’UI Conduktor, tu peux avoir:

* vue par **projet**
* vue par **domaine métier**
* vue par **topic**
* vue par **app** (exposure)
* vue “globale” pour la plateforme (santé, dette, tests manquants, etc.)

---

### 2.4. Centralisé ou fédéré: réponse courte

Donc pour répondre simplement à ta question:

* dbt aujourd’hui:

  * fonctionne très bien avec un **projet unique dans un repo unique**,
  * et supporte de plus en plus les **architectures multi-projets** (dbt Mesh) avec monorepo ou multi-repo. ([dbt Developer Hub][4])

* Ton “dbt for streaming” peut copier ce modèle:

  * laisser chaque team avoir son **propre repo** pour ses flux
  * connecter tous les projets via un **service Conduktor** qui:

    * récupère les projets Git
    * compile
    * construit un graph global
    * applique des règles globales
    * donne la visibilité complète sur tests, modèles, exposures et apps

Si tu veux, je peux te dessiner la “topologie Git” idéale pour une grande banque:

* noms de repos
* structure de dossiers par repo
* qui possède quoi (platform vs domain teams)
* et quelles règles sont checkées où (CI de repo vs contrôles globaux).

[1]: https://docs.getdbt.com/docs/build/projects?utm_source=chatgpt.com "About dbt projects | dbt Developer Hub - dbt Labs"
[2]: https://docs.getdbt.com/docs/cloud/git/version-control-basics?utm_source=chatgpt.com "Version control basics | dbt Developer Hub"
[3]: https://www.phdata.io/blog/run-dbt-models-centralized-vs-decentralized/?utm_source=chatgpt.com "How To Run dbt Models In a Centralized vs Decentralized ..."
[4]: https://docs.getdbt.com/best-practices/how-we-mesh/mesh-3-structures?utm_source=chatgpt.com "Deciding how to structure your dbt Mesh | dbt Developer Hub"
[5]: https://blog.infostrux.com/learn-how-to-organize-dbt-projects-b120af99042e?utm_source=chatgpt.com "Learn How To Organize dbt Projects | by Milan Mosny"
