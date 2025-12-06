dbt enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

Conduktor "dbt for Streaming" enables product and platform teams to design, secure, and monitor streaming data with the same declarative, test-driven practices.

On a construit dbt pour les flux opérationnels: une couche déclarative au-dessus de Kafka / Flink / Connect, où tes équipes décrivent leurs modèles temps réel, leurs tests, leurs règles de sécurité et leurs usages, et Conduktor compile ça vers ton runtime existant.
- Décrire les flux logiques (modèles) plutôt que des topics et des jobs éparpillés.
- Décrire les contrats: schémas, règles de qualité, sécurité, SLO.
- Décrire les usages: quelles apps, quels dashboards, quelles tables dépendent de quoi.
- Compiler tout ça en:
  - configs Proxy (virtual topics, masquage, crypto)
  - jobs Flink
  - connectors Sink
Tu parles en “layer de modélisation et de contrôle” pour tout ce qui touche au streaming.

Positioning VS classic GitOps?
More DSL → More AI capabilities to BUILD stuff
"Play" comme strategy PLG + "Play with AI" to even play more accessible
AE could play with the product, demo etc. with a clear "creation" factor leading to more understanding (than passive outcome like governance)
"Agents is the new app" (?)

Pipeline as product
- Fait → Les devs doivent posséder les pipelines de publication (via SDK, libs, CI/CD).
- Implication → Meilleure adoption car même stack, mêmes outils que pour du code.

An expansion of our declarative self-service
- today we declare applications and policies

we don't declare data:
- declare the "data" ("model") part of it
  - also define the security part of it
    - we don't have this consolidated today: it's exploded into interceptors (security) and various individual policies (DataQualityRule):
- multiple "data" can form a data product:
- declare dependencies (aka "exposure" in dbt)
  - schema impact
  - track data security exposure
  - + lineage & impact analysis
- declare data transformation/processing (proxy or flink or connect etc.)
- can write streaming tests
- AI: can write end to end declarations to build something (by declaring all models & materialization of data)
- schema: v1 v2: then what
- le core: easy (la tech, no UI)
- +le runtime (datbase fetch, fetch schemas)
- +but then: OSS ? UI/UX ?

pour qui? comment monétiser?
- persona différente??

what this is?
- graph of models (streams) + graph of permissions (between data and clients)
- + we have the runtime view (contrary to dbt)

+advantage of market awareness (what is dbt, how it works, very very often in our ICP)


# Models/Stream:

models: # stream: ?
  - name: payments_clean
    materialized: virtual_topic         # via Proxy
    cluster: prod-eu
    key: payment_id
    access: private
    group: finance   # ensure only finance can use this stream
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


  - name: payments_events
    config:
      contract:
        enforced: true # validate types of "columns"
    columns:
      - name: id
        data_type: integer
      ...

  - name: customer_balance_5m
    materialized: flink_stream                  # via Flink
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




  - name: fraud_events_warehouse
    materialized: sink                              # via Connect
    from:
      model: fraud_events_enriched
    sink:
      type: snowflake_table
      connector: snowflake-sink
      cluster: prod-eu
      topic: fraud.events.enriched
      config:
        snowflake_database: FRAUD
        snowflake_schema: PROD
        snowflake_table: FRAUD_EVENTS_ENRICHED
        key_mode: RECORD_KEY
        key_columns: ["payment_id"]
        insert_mode: UPSERT
        flush_interval_sec: 30
        max_retries: 5


exposures:        # downstream apps can be declared, to add to lineage and dependency resolution
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


  - name: payments_api
    type: application
    role: producer
    owner: team-payments
    language: java
    repo: https://git.internal/payments/api
    produces:
      - source("card_payments_raw")   // existing topic: payments.raw.v1
    sla:
      max_produce_latency_ms: 50
      availability: "99.9%"
    contracts:
      schema: strict          # refus des events qui ne match pas le schema
      versioning: "backward"  # contrainte de compatibilité

  - name: fraud_scoring_service
    type: application
    role: consumer
    owner: team-fraud
    language: python
    repo: https://git.internal/fraud/scoring
    consumes:
      - ref("card_payments_fraud_events") // a declared 'model' here
    sla:
      max_end_to_end_latency_ms: 800
      max_error_rate: 0.001
    access:
      allowed_roles: ["fraud-engine", "fraud-ml"]
      purpose: "fraud_detection"
    consumer_config:
      consumer_group: fraud-scoring-v3
      max_lag_messages: 10000

# Tests

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
Security
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
