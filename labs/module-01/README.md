# Module 1: AI-Driven Data Engineering Architecture

**Program:** Automating Data Engineering Pipelines with AI  
**Audience:** Experienced Data Engineers  
**Session:** Introductory day, 2 hours  
**Environment:** Prepared Windows VM, VS Code, GitHub Copilot, Python, Spark, Docker Compose, PostgreSQL, and Airflow  
**Agenda source:** [Program README, Module 1](../../README.md)

## 1. Purpose and trainer explanation

This session establishes the architecture and AI working model for the program. It is **not an ingestion, transformation, testing, or Airflow development lab**. Participants inspect the supplied project, use Copilot to reason about it, and map the reference architecture to their own enterprise environment.

### Trainer opening

> You already understand data engineering. Our focus is where AI can help across that lifecycle, what context it needs, and where engineering judgment and approval remain essential.
>
> We will use one retail example throughout the program. Today we will understand its architecture and identify useful AI opportunities. Later modules will build ingestion, transformations, metadata, quality checks, orchestration, monitoring, and documentation.
>
> AI-assisted does not mean AI-controlled. A plausible explanation is not proof that a component exists or that a pipeline is correct.

### Learning objectives

By the end of this session, participants can:

1. Position batch, streaming, API, and CDC pipelines within lake, warehouse, and lakehouse architectures.
2. Identify AI opportunities across discovery, development, testing, documentation, monitoring, and optimization.
3. Distinguish an LLM, an AI assistant, a tool-using agent, and deterministic automation.
4. Explain how repository context, metadata, business rules, RAG, tools/APIs, and MCP can support grounded assistance.
5. Trace the training reference architecture and distinguish existing components from future implementation.
6. Map the architecture to their own enterprise stack and define human approval boundaries.

### Scope alignment with the program agenda

| Module 1 agenda topic | Coverage in this session |
|---|---|
| Modern Data Engineering Landscape | Activity 2: architecture and ingestion-pattern comparison |
| AI Opportunities Across the DE Lifecycle | Activity 3: opportunity and risk classification |
| AI Assistant, LLM and Agent Concepts | Activity 4: concepts and grounded Copilot exercise |
| Reference Architecture | Activity 5: guided walkthrough of the supplied retail project |
| Hands-on: Architecture Mapping | Activity 6: reference-to-enterprise mapping and approval boundaries |

**Defer to later modules:** ingestion code and source validation (Module 2), SQL/PySpark transformations (Module 3), context packages and lineage analysis (Module 4), quality tests (Module 5), DAG development and DataOps (Module 6), fault injection/RCA/optimization (Module 7), and runbooks/agentic capstone (Module 8).

## 2. Trainer preparation: before participants arrive

Environment setup is a **pre-session responsibility**, not a participant activity that consumes the two-hour introduction.

- [ ] Open `C:\AI-DataEngineering-Training` in VS Code.
- [ ] Confirm participants can access GitHub Copilot Chat through approved accounts.
- [ ] Confirm the prepared Python 3.11 virtual environment, PySpark 3.5.4, and Java 17.
- [ ] Confirm Docker Desktop and the prepared Compose services are ready.
- [ ] Confirm the existing environment smoke script and smoke DAG work.
- [ ] Open the three supplied retail CSVs and the project files listed in Activity 5.
- [ ] Prepare an example enterprise mapping for participants who cannot discuss their own platform.
- [ ] Remind participants not to paste credentials, production data, or confidential architecture into Copilot.

**No additional software or cloud subscription is required.** Participants do not install packages, rebuild images, configure MCP servers, set up RAG, or create cloud resources.

### Files the trainer must create

**None beyond making this lab document available.** The exercises use existing project files.

Participants create one new output during the session: **`docs\module-01-architecture.md`**. This is a new worksheet to create, not an existing input or executable pipeline file. The trainer does not need to pre-create it.

### Trainer-only preflight commands

Run in PowerShell from the project root before delivery:

```powershell
Set-Location 'C:\AI-DataEngineering-Training'
.\.venv\Scripts\python.exe --version
.\.venv\Scripts\python.exe -c "import pyspark; print(pyspark.__version__)"
java -version
docker compose ps -a
```

Expected: Python `3.11.x`, PySpark `3.5.4`, Java `17.x`; both PostgreSQL services healthy, Airflow scheduler/webserver running, and `airflow-init` exited successfully with code `0`.

**Known preparation issue:** at lab authoring, the host virtual environment reported PySpark `4.2.0`, while the existing scheduler container reported `3.5.4`. The host dependency list is unpinned. The trainer must reconcile the prepared VM or use the verified container for any optional Spark demonstration; do not ask participants to change dependencies.

Existing smoke commands, if needed by the trainer:

```powershell
.\.venv\Scripts\python.exe .\scripts\spark_smoke_test.py
docker compose exec -T airflow-scheduler python -c "import sys, pyspark; print(sys.version.split()[0]); print(pyspark.__version__)"
docker compose exec -T airflow-scheduler python /opt/airflow/project/scripts/spark_smoke_test.py
docker compose exec -T airflow-scheduler airflow dags list
docker compose exec -T airflow-scheduler airflow dags list-import-errors
```

Run the host smoke test only with awareness of its actual runtime version. Expected smoke output is two rows, `1 / A` and `2 / B`, with process exit code `0`. Airflow should list `training_environment_smoke_test` without import errors. These checks prove environment readiness, not retail processing. Do not run a lengthy troubleshooting session in front of the class; participants can complete the architecture exercises by reading the supplied files.

## 3. Two-hour agenda

| Activity | Time | Duration | Format / output |
|---|---|---:|---|
| 1. Orientation and worksheet | 00:00-00:10 | 10 min | Business question and learning goals |
| 2. Modern DE landscape | 00:10-00:25 | 15 min | Short explanation and pattern classification |
| 3. AI opportunities across the lifecycle | 00:25-00:40 | 15 min | Pair exercise: opportunity/risk matrix |
| 4. Assistants, LLMs, agents, and grounding | 00:40-00:55 | 15 min | Copilot prompt and context comparison |
| 5. Reference-project architecture walkthrough | 00:55-01:15 | 20 min | File inspection and source-to-report diagram |
| 6. Hands-on enterprise architecture mapping | 01:15-01:45 | 30 min | Platform mapping, AI touchpoints, approval boundaries |
| 7. Share-back, validation, and knowledge check | 01:45-01:55 | 10 min | Peer review and discussion |
| 8. Transition to Module 2 | 01:55-02:00 | 5 min | Ingestion handoff |
| **Total** | | **120 min** | |

Use pairs where possible: one participant drives Copilot, the other verifies claims. Swap roles for the enterprise mapping exercise. Keep detailed implementation questions in a parking lot for the relevant later module.

## 4. Step-by-step activities

### Activity 1: Orientation and worksheet (10 minutes)

**Trainer: 5 minutes**

Introduce the retail business question:

> How can we build a reliable customer/order/product analytics pipeline and use AI to accelerate its engineering lifecycle without giving it uncontrolled production authority?

Explain the session boundary: **understand, map, and evaluate**, rather than implement.

**Participants: 5 minutes**

1. Open the supplied project in VS Code.
2. Open [customers.csv](../../data/raw/customers.csv), [orders.csv](../../data/raw/orders.csv), and [products.csv](../../data/raw/products.csv).
3. In VS Code, create the new file `docs\module-01-architecture.md`.
4. Paste these worksheet headings:

   ```markdown
   # Module 1 architecture worksheet
   ## 1. Business question and existing project facts
   ## 2. Architecture and ingestion patterns
   ## 3. AI opportunities and risks
   ## 4. Grounding observations
   ## 5. Reference architecture
   ## 6. Enterprise platform mapping
   ## 7. Human approval boundaries
   ## 8. Module 2 handoff
   ```

**Expected output:** A saved worksheet and a shared understanding that no business pipeline is being implemented today.

### Activity 2: Modern Data Engineering landscape (15 minutes)

**Trainer explanation: 7 minutes**

Use participants' existing experience rather than teaching ETL fundamentals:

| Architecture | Emphasis | Discussion point |
|---|---|---|
| Traditional ETL / warehouse | Curated relational models and controlled loading | AI can help explain mappings and generate reviewed SQL |
| Data lake | Flexible file-based storage with multiple consumption paths | Schema, discoverability, and quality do not disappear |
| Cloud-native warehouse | Managed analytical compute/storage and SQL serving | AI must respect platform semantics, access, and cost |
| Lakehouse | Lake storage with table-management and analytical capabilities | A folder of CSV or Parquet files alone is not a lakehouse |

Clarify that batch/streaming/API/CDC describe different dimensions: an API can be polled in a batch, and CDC can be delivered through a stream. They are not mutually exclusive architectures.

Explain the cross-cutting roles:

- **Metadata:** technical structure, business meaning, and operational context.
- **Data quality:** rules and evidence that data is fit for use.
- **Lineage:** upstream/downstream dependencies and impact.
- **Observability:** freshness, volumes, failures, duration, and other signals.
- **DataOps:** repeatable development, review, testing, release, and operation.

**Pair exercise: 8 minutes**

Classify these scenarios in worksheet section 2:

1. Daily retail CSV extracts: batch file ingestion.
2. Product catalog obtained from a REST endpoint each hour: scheduled API ingestion.
3. Order changes captured from a database log: CDC; delivery and processing cadence must be specified.
4. Continuous checkout events: streaming ingestion.

For each, name one architectural implication, such as schema evolution, checkpoints, deletes, latency, or retry behavior.

**Validation:** All four scenarios are classified, and the pair explains why the current CSV fixture exercise does not implement API ingestion, streaming, or CDC.

### Activity 3: AI opportunities across the lifecycle (15 minutes)

**Trainer explanation: 5 minutes**

Distinguish:

- **AI-assisted development:** an engineer asks for explanations or drafts, checks them, and decides what to accept.
- **Deterministic automation:** an approved workflow executes predefined steps, such as an Airflow task; it need not use an LLM.
- **Agentic automation:** a model reasons about a goal, selects tools, inspects results, and may act within an explicitly limited scope.

**Pair exercise: 10 minutes**

Attach the three CSVs to Copilot Chat using **Add Context** or the equivalent file-attachment control. Use approved synthetic project context only.

#### Copilot prompt 1: Opportunity discovery

```text
Act as a Data Engineering architecture assistant.
We are in an introductory architecture session, not an implementation lab.
Our reference stack is VS Code, GitHub Copilot, Python, PySpark, Airflow,
PostgreSQL, and local CSV files. Use the attached retail CSVs as context.
Do not edit files, execute commands, or suggest installations.
Build a table covering discovery, ingestion development, SQL/PySpark generation,
testing, documentation, monitoring, and optimization.
For each, state one AI opportunity, required context, expected benefit,
failure risk, and human approval boundary.
Distinguish low-risk read-only assistance from production-changing actions.
Do not claim these capabilities are already implemented in this project.
```

Select three opportunities and record them in worksheet section 3:

| Example opportunity | Risk / boundary |
|---|---|
| Explain supplied schemas | Low-risk read-only assistance; verify against files |
| Draft a transformation | Review and test before accepting; no execution today |
| Change production schema or remediate a failed pipeline | High-impact action requiring scoped permissions and explicit approval |

**Validation:** Each chosen opportunity names the necessary context, a likely failure, and an approval point. Risk is judged by data sensitivity and action impact, not just by whether the model sounds confident.

### Activity 4: Assistants, LLMs, agents, and grounding (15 minutes)

**Trainer explanation: 5 minutes**

- An **LLM** generates outputs from context; it does not inherently know your current repository or database state.
- An **assistant**, such as GitHub Copilot or Claude Code, provides an interface around models and available context/tools. This lab uses GitHub Copilot.
- An **agent** adds a goal-directed loop involving reasoning, tool selection, observations, and actions. Tool availability does not imply unrestricted permission.
- SQL, Python, PySpark, Airflow, and documentation are possible generated artifacts, not automatically correct deliverables.
- **Grounding** supplies relevant source files, metadata, business rules, standards, and operational evidence.
- **RAG** retrieves relevant context before generation; manually attaching files demonstrates context selection, not a deployed RAG system.
- **Tools/APIs** expose actions or queries; **MCP** is a standardized connectivity pattern through which an AI client can access tools/resources. It does not itself provide a catalog, accurate reasoning, or production authorization.

**Hands-on comparison: 10 minutes**

In a fresh Copilot conversation without file attachments, paste:

#### Copilot prompt 2A: Observe missing context

```text
Without inspecting workspace files or using tools, describe what you can
and cannot know about this project's retail pipeline architecture.
Do not guess filenames, schemas, tables, or implemented capabilities.
List the context you would need to give a grounded answer.
```

Then attach the three CSVs, [spark_smoke_test.py](../../scripts/spark_smoke_test.py), and [training_smoke_dag.py](../../dags/training_smoke_dag.py).

#### Copilot prompt 2B: Ground the answer

```text
Now use only these attached files. Do not edit or execute anything.
Separate observed facts, proposed components, and unknowns.
Identify the source entities and the actual behavior of the two Python files.
Does either file read the retail CSVs or publish data to PostgreSQL?
Cite file names and columns or callable names for your claims.
State what further metadata and business rules would be needed to design
the retail pipeline. Do not infer a full implementation from installed tools.
```

**Expected output:** The grounded answer identifies two hardcoded Spark smoke checks and three source fixtures; it does not invent a retail processing DAG.

**Validation:** Record two facts verified against files and one unknown. Explain what improved with context. If Copilot still has automatic workspace context, treat the first response as a stated no-inspection exercise, not a controlled model benchmark.

### Activity 5: Reference-project architecture walkthrough (20 minutes)

**Trainer walkthrough: 10 minutes**

Inspect these existing resources:

| Resource | What exists now |
|---|---|
| [customers.csv](../../data/raw/customers.csv) | 3 customers: ID, name, region, signup date |
| [orders.csv](../../data/raw/orders.csv) | 5 order rows: customer/product references, date, quantity, price, status |
| [products.csv](../../data/raw/products.csv) | 3 products: ID, name, category |
| [spark_smoke_test.py](../../scripts/spark_smoke_test.py) | Local Spark check using hardcoded `id`/`name` rows |
| [training_smoke_dag.py](../../dags/training_smoke_dag.py) | DAG `training_environment_smoke_test`, task `check_python_pandas_spark`; hardcoded customer/revenue rows |
| [docker-compose.yml](../../docker-compose.yml) | Retail PostgreSQL plus separate Airflow metadata PostgreSQL, initialization, scheduler, and webserver services |
| [Dockerfile.airflow](../../Dockerfile.airflow) | Prepared container runtime for Airflow/Python/Spark |

Do not paste configuration credentials into Copilot.

Explain the two connected views:

```text
Engineering / control view
Business requirement + metadata + standards
                  |
                  v
AI assistant -> engineer review -> code repository / version control
                                      |
                                      v
                             tests + release approval
                                      |
                                      v
                            orchestration and execution
                                      |
                                      v
                         logs / metrics / human feedback

Proposed retail data flow
CSV fixtures now; PostgreSQL source exercises in later modules
                  |
                  v
             ingestion
                  |
                  v
       validation + PySpark processing
                  |
                  v
       curated file / relational storage
                  |
                  v
           SQL reporting consumer

Airflow coordinates tasks; Spark processes rows.
Monitoring observes execution and data signals.
Airflow metadata PostgreSQL stores orchestration state, not retail reports.
```

The [program architecture](../../README.md) includes PostgreSQL as a representative **source** database. The existing `data-postgres` service provides the `retail` database, but the supplied smoke code does not establish populated source tables. Relational serving is a possible later design choice, not an already-built reporting layer. If the same service is eventually used for training source and target data, explicitly separate those roles and namespaces.

The current code does not ingest the CSVs, create retail tables, generate curated outputs, or serve reports. No dashboard application, RAG system, MCP integration, or business pipeline agent is provided.

**Participant exercise: 10 minutes**

1. Add an ASCII source-to-report diagram to worksheet section 5.
2. Mark each box `EXISTING`, `PROPOSED`, or `NOT PART OF THIS LAB`.
3. Label orchestration, processing, storage, and monitoring responsibilities.
4. Add a separate assistant-to-engineer-to-repository path. AI is not required in the runtime data path.
5. State one example business rule that needs approval, such as whether cancelled orders contribute to revenue. Do not implement the calculation today.

**Validation:** A peer can trace a retail record from source to reporting in the proposed flow and locate the implemented baseline separately. No nonexistent code, table, reporting tool, or configured integration is presented as available.

### Activity 6: Hands-on enterprise architecture mapping (30 minutes)

This is the principal Module 1 lab specified in the program agenda.

#### Step 1: Map the platform roles (10 minutes)

In worksheet section 6, create a table with these columns:

`Role | Training reference | My enterprise equivalent | Difference / open question`

Use your own approved, nonconfidential environment description. If unavailable, choose one illustrative stack below. These are role mappings, not exact product equivalences or provisioned resources.

| Role | Training reference | Illustrative enterprise options |
|---|---|---|
| AI assistance | GitHub Copilot in VS Code | Organization-approved assistant and access controls |
| Repository and change workflow | Supplied project; Git workflow introduced later | GitHub, Azure Repos, or another approved repository |
| Orchestration | Airflow | Databricks Workflows, ADF pipelines, Glue workflows, Snowflake Tasks, or managed Airflow, depending on scope |
| Processing | PySpark | Databricks Spark, Glue Spark, SQL/Snowpark where appropriate |
| Storage / serving | Local files and PostgreSQL service | ADLS/S3, Delta/Iceberg tables, warehouse tables, or Snowflake storage |
| Metadata / lineage | Inspectable files today; richer context later | Unity Catalog, Purview, Glue Data Catalog, or platform catalogs |
| Monitoring | Task logs and service visibility | Enterprise observability and platform monitoring |
| Reporting | Proposed SQL consumer | Existing organizational SQL/BI consumption layer |

Note at least one non-equivalence: ADF orchestration does not automatically replace Spark processing; a catalog is not identical to end-to-end lineage; warehouse SQL is not a drop-in replacement for all PySpark behavior.

#### Step 2: Ask Copilot to review the mapping (8 minutes)

Attach the worksheet and relevant source files. Replace the bracketed text before pasting.

#### Copilot prompt 3: Enterprise mapping

```text
Review my architecture mapping for an introductory Data Engineering session.
My enterprise stack, using only approved nonconfidential details, is:
[insert platform names and roles, or select one illustrative stack].
Use the attached worksheet and project files as evidence.
Map AI assistance, repository, orchestration, processing, storage,
metadata/lineage, monitoring, and reporting.
For each role, identify an equivalent capability, an important difference,
and an open question. Do not imply products are exact substitutes.
Mark proposed or unavailable capabilities clearly.
Suggest where retrieved metadata/context, tools/APIs, and MCP-style connectivity
could fit conceptually; do not configure or require them.
Do not edit files, execute commands, provision resources, or invent existing assets.
```

#### Step 3: Add AI touchpoints and approval boundaries (7 minutes)

In worksheet section 7:

1. Mark three AI touchpoints, such as discovery, code drafting, and log explanation.
2. Show where repository/business context is supplied.
3. Show where a future metadata retrieval service or approved tool connector could fit; label it `CONCEPTUAL`.
4. Choose three actions and classify their permission boundary:

   | Action | Suggested boundary |
   |---|---|
   | Explain approved synthetic source files | Read-only assistance |
   | Generate or edit a proposed pipeline change | Scoped development permission; human review and tests before merge |
   | Deploy, change production schemas, delete data, or remediate production | Explicit authorization, least privilege, auditability, and rollback plan |

#### Copilot prompt 4: Approval-boundary review

```text
Review this architecture's AI action boundaries without taking any action.
For each proposed AI touchpoint, state:
required context, read/write permissions, human approval point,
validation evidence, and a reason to stop or escalate.
Distinguish assistant recommendations from agent tool execution.
Identify one high-value low-risk opportunity and one high-impact action
that must not be autonomous by default.
Keep this conceptual; do not generate deployment or remediation commands.
```

#### Step 4: Pair validation (5 minutes)

Partners verify:

- All eight platform roles are mapped or explicitly marked unknown.
- At least one non-equivalence is explained.
- Three AI touchpoints include required context and action boundaries.
- RAG/MCP are conceptual extensions, not claimed implementations.
- No production secrets or confidential system details are included.

**Expected output:** A reviewed enterprise mapping that preserves the pattern even when platform names differ.

### Activity 7: Share-back and knowledge check (10 minutes)

**Share-back: 5 minutes**

Ask two pairs for a two-minute explanation each:

1. One architecture mapping that transfers well.
2. One place where the enterprise platform changes the design.
3. One AI opportunity and its approval boundary.

Use the remaining minute to correct any confusion between existing code and proposed capabilities.

**Knowledge check: 5 minutes**

1. **Does a successful Spark/Airflow smoke check prove the retail pipeline works?**  
   **Answer:** No. The existing checks process hardcoded rows; they do not ingest the supplied sources or validate retail business results.

2. **How is an assistant different from an agent?**  
   **Answer:** Assistance may stop at explanation or drafts. An agent uses a goal-directed reasoning/tool loop and can act, subject to explicitly scoped permissions and approval.

3. **Is an hourly API pull batch or streaming?**  
   **Answer:** Usually scheduled batch API ingestion. API is an interface, not a guarantee of streaming processing.

4. **What makes an AI-generated architecture grounded?**  
   **Answer:** Claims trace to supplied code, metadata, business rules, and observed operational evidence; unknowns and proposed components remain explicit.

5. **Are RAG and MCP the same thing?**  
   **Answer:** No. RAG retrieves context for generation; MCP provides a standardized connection pattern for tools/resources. Neither guarantees correct outputs or authorizes production actions.

6. **Where should human approval appear?**  
   **Answer:** At consequential boundaries such as accepting business rules, merging generated changes, deploying, changing schemas, or performing production remediation.

## 5. Expected outputs and completion validation

Only one participant file is required: the newly created `docs\module-01-architecture.md`.

- [ ] All eight worksheet sections contain useful notes.
- [ ] Four ingestion scenarios are classified with an architectural implication.
- [ ] Three AI opportunities identify context, risk, and approval.
- [ ] Grounding exercise records two verified facts and one unknown.
- [ ] Source-to-report diagram separates existing from proposed components.
- [ ] Enterprise mapping covers eight roles and one non-equivalence.
- [ ] Three AI touchpoints have explicit permission/approval boundaries.
- [ ] Module 2 handoff identifies sources and questions, not unimplemented code presented as working.

**No executable pipeline, database write, new DAG, detailed quality test suite, or dashboard is an acceptance requirement for Module 1.** Validation is evidence-based file inspection and peer explanation. Later modules validate implementation behavior.

## 6. Common mistakes and troubleshooting

| Issue | Response |
|---|---|
| Spending the introduction on package installation or Docker repair | Trainer completes preflight beforehand; continue architecture work from files if services are unavailable |
| Copilot cannot access the intended files | Use Add Context/file attachments; ask it to state inaccessible context rather than guess |
| Copilot invents pipeline modules or retail tables | Request file evidence and separate observed/proposed/unknown claims |
| Treating a smoke DAG as the business pipeline | Read its callable and identify the hardcoded rows |
| Mapping every enterprise role to one product | Separate orchestration, processing, storage, catalog, and monitoring capabilities |
| Calling a folder of files a lakehouse | Discuss required table-management and analytical capabilities; the local folder is a training simplification |
| Treating MCP access as production permission | Apply least privilege and explicit action approval; connectivity is not authorization |
| Treating attachments as a complete RAG deployment | Explain that this is manual context selection only |
| Pasting confidential platform details | Replace them with approved generic roles and synthetic examples |
| Going deep into joins, CDC implementation, retries, or fault injection | Capture the question and route it to the relevant later module |
| Windows host runtime differs from the prepared container | Trainer resolves or demonstrates the verified container; no participant dependency changes |

**Fallback if Copilot is unavailable:** use the same prompts as pair-review questions, record the limitation, and complete the mapping manually. Do not claim that an AI exercise was executed.

## 7. Transition to Module 2: AI-Assisted Data Ingestion (5 minutes)

The [program agenda](../../README.md) defines Module 2 as **AI-Assisted Data Ingestion**. Its [lab document](../module-02/README.md) is currently a template and must be prepared before that session.

Have participants add these handoff items to worksheet section 8:

1. Known sources: the supplied customer, order, and product CSVs; PostgreSQL source exercises are part of the next module's preparation.
2. Questions to resolve: authoritative schemas, key semantics, business meaning, ingestion cadence, and full versus incremental behavior.
3. Proposed next work: use AI for source discovery and ingestion code, then add configuration, logging, error handling, and basic validation.
4. Guardrail: generated code must be reviewed and validated; no assumption that database source tables are already populated.

**Trainer preparation for Module 2:** verify or provide its PostgreSQL source tables, seed data, and exact instructions before delivery. Those assets are not prerequisites for today's architecture mapping and are not claimed to exist here.

### Closing statement

> Today we mapped where AI fits, what context it needs, and what it must not do without approval. In Module 2 we will move from architectural understanding to AI-assisted ingestion implementation, using the same retail example and evidence-based review discipline.
