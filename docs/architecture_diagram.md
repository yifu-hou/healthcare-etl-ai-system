# System Architecture

## High-Level Architecture
```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                             │
├─────────────────────────────────────────────────────────────┤
│  FHIR JSON Files  │  Lab Results CSV  │  Appointments CSV   │
│  (Synthea)        │                   │                     │
└──────────┬────────┴───────────┬───────┴──────────┬──────────┘
           │                    │                  │
           └────────────────────┼──────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │   ETL PIPELINE        │
                    │   (Python)            │
                    ├───────────────────────┤
                    │ • Extract (FHIR/CSV)  │
                    │ • Transform (Validate)│
                    │ • Load (Dual-Write)   │
                    └───────┬───────────────┘
                            │
              ┌─────────────┴─────────────┐
              │                           │
    ┌─────────▼─────────┐      ┌─────────▼──────────┐
    │  SALESFORCE CRM   │      │  BIGQUERY DW       │
    │  (Operational)    │      │  (Analytics)       │
    ├───────────────────┤      ├────────────────────┤
    │ • Patients        │      │ • Patient Snapshot │
    │ • Lab Results     │      │ • Clinical Events  │
    │ • Care Plans      │      │ • Risk History     │
    │ • Risk Assessments│      │ • Trend Analysis   │
    └─────────┬─────────┘      └─────────┬──────────┘
              │                          │
              └──────────┬───────────────┘
                         │
                ┌────────▼────────┐
                │   AI AGENT      │
                │ (Vertex AI)     │
                ├─────────────────┤
                │ • Gemini 2.5    │
                │ • Query Router  │
                │ • NL Interface  │
                └─────────────────┘
```

## Data Flow

### 1. Ingestion Flow
```
Raw Data → Parser → Validator → Mapper → Loader → [Salesforce + BigQuery]
```

### 2. Query Flow
```
User Question → Agent → Tool Selection → Data Query → AI Synthesis → Answer
```

## Components Detail

### ETL Pipeline
- **Language:** Python 3.11
- **Extract:** FHIR parser, CSV reader
- **Transform:** Data mapper, validator, risk calculator
- **Load:** Salesforce API, BigQuery streaming

### Salesforce CRM
- **Edition:** Developer Edition
- **Objects:** 4 custom (Patient, Lab, Care Plan, Risk)
- **API:** REST API with OAuth
- **Purpose:** Operational patient management

### BigQuery Data Warehouse
- **Tables:** 3 (patients_snapshot, clinical_events, risk_scores_history)
- **Purpose:** Historical analytics, trend analysis
- **Schema:** Star schema with time-series data

### AI Agent
- **Model:** Vertex AI Gemini 2.5
- **Tools:** Salesforce Tool, BigQuery Tool
- **Capabilities:** 
  - High-risk patient identification
  - Lab result analysis
  - Population health trends
  - Natural language queries

## Technology Stack

| Layer | Technology |
|-------|------------|
| **Language** | Python 3.11 |
| **Data Processing** | pandas, json |
| **CRM** | Salesforce (simple-salesforce) |
| **Data Warehouse** | Google BigQuery |
| **AI/ML** | Vertex AI (Gemini 2.5) |
| **Version Control** | Git/GitHub |
| **Environment** | Virtual Environment (venv) |

## Security & Compliance

- Service account authentication (GCP)
- OAuth 2.0 (Salesforce)
- Environment variable management (.env)
- API rate limiting
- Audit logging
- HIPAA-ready architecture (encryption at rest/transit)

## Scalability Considerations

- Batch processing for large datasets
- Upsert pattern for idempotency
- Partitioned BigQuery tables
- Stateless agent design
- Horizontal scaling ready
