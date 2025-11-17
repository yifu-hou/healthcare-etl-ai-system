# healthcare-etl-ai-system
Healthcare data pipeline with Salesforce integration and AI agent

## Workflow 

#### Phase 0: Environment Setup 


#### Phase 1: Data Generation & Preparation

Generate synthetic patient data using Synthea (FHIR format)
Create sample CSV files (lab results, appointments)
Store raw data in data/raw/ folder

- Synthea FHIR Data:
Patient demographics (name, DOB, gender, address)
Medical conditions
Medications
Observations (vitals)
Encounters (visits)

- Lab Results CSV:

Patient ID, test type, values, dates
Reference ranges, status

- Appointments CSV:

Patient ID, dates, providers
Appointment types, status

#### Phase 2: Salesforce Setup

Create custom objects in Salesforce (Patient Medical Record, Care Plan, Lab Results, Risk Assessment)
Configure fields and relationships for each object
Set up sample dashboard for visualization

#### Phase 3: Build ETL Pipeline

Extract: Read FHIR/CSV files, parse data
Transform: Clean data, map to Salesforce schema, calculate risk scores
Load: Push data to Salesforce via REST API
Test pipeline with sample data

#### Phase 4: Data Warehouse Setup

Create BigQuery tables (patient snapshots, clinical events, metrics)
Export data from Salesforce to BigQuery
Set up scheduled sync between Salesforce and BigQuery

#### Phase 5: Build AI Agent

Create agent tools: Functions to query Salesforce and BigQuery
Connect to Vertex AI (Gemini model)
Implement agent logic: Natural language → tool selection → API calls → response
Test agent queries (e.g., "Which patients need follow-up?")

#### Phase 6: Integration & Testing

End-to-end test: Raw data → ETL → Salesforce → BigQuery → AI Agent query
Create demo scenarios (risk alerts, care recommendations)
Document everything (README, architecture diagram, demo video)


## System Structure
```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES LAYER                       │
├─────────────────────────────────────────────────────────────────┤
│  Synthea FHIR Data  │  Lab Results CSV  │  Appointment Files    │
└──────────┬──────────┴───────────┬────────┴──────────┬───────────┘
           │                      │                   │
           └──────────────────────┼───────────────────┘
                                  ▼
           ┌──────────────────────────────────────────┐
           │         ETL PIPELINE (Python)            │
           ├──────────────────────────────────────────┤
           │  • Extract: File readers, API clients    │
           │  • Transform: Data mapping, validation   │
           │  • Load: Salesforce API connector        │
           └──────────┬───────────────────┬───────────┘
                      │                   │
         ┌────────────▼─────────┐         │
         │   SALESFORCE CRM     │         │
         ├──────────────────────┤         │
         │ • Contacts (Patients)│         │
         │ • Custom: Medical    │         │
         │   Records, Care Plans│         │
         │ • Cases (Episodes)   │         │
         │ • Dashboards         │         │
         └────────┬─────────────┘         │
                  │                       │
                  │ Sync/Export           │
                  │                       │
                  ▼                       ▼
         ┌─────────────────────────────────┐
         │      BigQuery (Warehouse)       │
         ├─────────────────────────────────┤
         │ • Historical patient data       │
         │ • Aggregated metrics            │
         │ • Analytics tables              │
         └────────┬────────────────────────┘
                  │
                  │ Query
                  │
         ┌────────▼────────────────────────┐
         │    AGENTIC AI LAYER             │
         │    (Vertex AI + Gemini)         │
         ├─────────────────────────────────┤
         │  Agent Capabilities:            │
         │  • Query Salesforce via API     │
         │  • Analyze BigQuery data        │
         │  • Natural language interface   │
         │  • Generate recommendations     │
         └────────┬────────────────────────┘
                  │
                  ▼
         ┌─────────────────────────────────┐
         │      USER INTERFACE             │
         ├─────────────────────────────────┤
         │ • Salesforce Dashboard          │
         │ • Python CLI for AI queries     │
         │ • Simple Streamlit app (opt)    │
         └─────────────────────────────────┘
```

## Project Directory Structure

```
healthcare-etl-ai-system/
│
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
│
├── docs/
│   ├── architecture_diagram.png
│   ├── data_flow.md
│   ├── setup_guide.md
│   └── demo_walkthrough.md
│
├── data/
│   ├── raw/                      # Original source files
│   │   ├── synthea_output/
│   │   ├── lab_results.csv
│   │   └── appointments.csv
│   ├── processed/                # Cleaned data
│   └── sample_outputs/           # Example results
│
├── config/
│   ├── salesforce_config.yaml
│   ├── bigquery_config.yaml
│   └── vertex_ai_config.yaml
│
├── etl/
│   ├── __init__.py
│   ├── extract/
│   │   ├── __init__.py
│   │   ├── synthea_reader.py
│   │   ├── csv_reader.py
│   │   └── fhir_parser.py
│   ├── transform/
│   │   ├── __init__.py
│   │   ├── data_mapper.py
│   │   ├── validator.py
│   │   └── risk_calculator.py
│   └── load/
│       ├── __init__.py
│       ├── salesforce_loader.py
│       └── bigquery_loader.py
│
├── salesforce/
│   ├── __init__.py
│   ├── api_client.py
│   ├── object_schemas.py         # Salesforce object definitions
│   └── query_builder.py
│
├── ai_agent/
│   ├── __init__.py
│   ├── agent.py                  # Main agent orchestration
│   ├── tools/
│   │   ├── __init__.py
│   │   ├── salesforce_tool.py   # Queries Salesforce
│   │   ├── bigquery_tool.py     # Queries warehouse
│   │   └── analysis_tool.py     # Data analysis functions
│   ├── prompts/
│   │   └── system_prompts.py
│   └── vertex_client.py
│
├── pipeline/
│   ├── __init__.py
│   ├── orchestrator.py           # Main ETL runner
│   └── scheduler.py              # Scheduling logic
│
├── utils/
│   ├── __init__.py
│   ├── logger.py
│   ├── error_handler.py
│   └── helpers.py
│
├── tests/
│   ├── test_etl.py
│   ├── test_salesforce.py
│   └── test_agent.py
│
├── notebooks/
│   ├── data_exploration.ipynb
│   └── agent_testing.ipynb
│
└── scripts/
    ├── setup_salesforce_objects.py
    ├── generate_sample_data.py
    ├── run_etl_pipeline.py
    └── run_ai_agent.py
```

## Detailed System Components

### 1. **Data Flow Architecture**

**Batch ETL Flow:**

```
Raw Data → Extract → Transform → Validate → Load to Salesforce
                                         ↓
                                    Load to BigQuery
```

**AI Agent Query Flow:**

```
User Query → Agent (Vertex AI) → Tool Selection → API Calls 
                                                    ↓
                              Salesforce/BigQuery Data
                                                    ↓
                              Analysis & Response Generation
