# Salesforce Schema Documentation

## Custom Objects Overview

### 1. Patient Medical Record (`Patient_Medical_Record__c`)
**Purpose:** Store core patient demographic and contact information

**Fields:**
- `Patient_ID__c` (Text, External ID) - Unique patient identifier
- `First_Name__c` (Text) - Patient first name
- `Last_Name__c` (Text) - Patient last name
- `Date_of_Birth__c` (Date) - Patient date of birth
- `Gender__c` (Picklist) - Male, Female, Other
- `Email__c` (Email) - Patient email address
- `Phone__c` (Phone) - Patient phone number
- `Address__c` (Long Text Area) - Patient address

---

### 2. Lab Result (`Lab_Result__c`)
**Purpose:** Store laboratory test results

**Fields:**
- `Patient__c` (Lookup → Patient Medical Record) - Related patient
- `Test_Type__c` (Text) - Type of test (A1C, Glucose, etc.)
- `Test_Value__c` (Number) - Numeric test result
- `Reference_Range__c` (Text) - Normal range for the test
- `Test_Date__c` (Date) - Date test was performed
- `Status__c` (Picklist) - Normal, Abnormal, Critical

---

### 3. Care Plan (`Care_Plan__c`)
**Purpose:** Track patient treatment plans

**Fields:**
- `Patient__c` (Lookup → Patient Medical Record) - Related patient
- `Plan_Name__c` (Text) - Name of the care plan
- `Start_Date__c` (Date) - Plan start date
- `End_Date__c` (Date) - Plan end date
- `Status__c` (Picklist) - Active, Completed, Cancelled, On Hold
- `Goals__c` (Long Text Area) - Care plan goals and objectives

---

### 4. Risk Assessment (`Risk_Assessment__c`)
**Purpose:** Store calculated patient risk scores

**Fields:**
- `Patient__c` (Lookup → Patient Medical Record) - Related patient
- `Risk_Level__c` (Picklist) - Low, Medium, High, Critical
- `Risk_Score__c` (Number) - Numeric risk score (0-100)
- `Assessment_Date__c` (Date) - Date of assessment
- `Risk_Factors__c` (Long Text Area) - Contributing risk factors

---

## Data Model Relationships
```
Patient Medical Record (Parent)
    ├── Lab Results (Children)
    ├── Care Plans (Children)
    └── Risk Assessments (Children)
```

All child objects have a Lookup relationship to Patient Medical Record.