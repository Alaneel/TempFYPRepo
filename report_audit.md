# FYP Final Report Audit & Technical Verification

This document summarizes the technical audit of the **FYP Final Report Draft** against the actual project implementation.

## 1. Technical Accuracy Check

| Component / Claim | Verified in Codebase? | Status | Notes |
| :--- | :---: | :--- | :--- |
| **Hybrid Scoring Weights** | ✅ | **Match** | Type (0.2), District (0.15), Price (0.2), Beds (0.15), Bargain (0.15), Facilities (0.15). Sum = 1.0. |
| **Bargain Score Formula** | ✅ | **Match** | `(estimate - price) / estimate`, clamped to [-0.5, 0.5], shifted to [0, 1]. |
| **XGBoost MAPE** | ✅ | **Match** | 18.0% for Condo Sale, 18.1% for Condo Rent. Correctly cited in Table 5.4. |
| **NDCG@5 Scores** | ✅ | **Match** | User A: 0.500, User B: 1.000, User C: 1.000. Mean: 0.833. |
| **Property AI Assistant** | ✅ | **Match** | Implemented using Claude 4.5 Haiku with live context (listing data + valuation + SHAP) injection. |
| **Geospatial Map** | ✅ | **Match** | Fully functional Leaflet.js implementation with custom markers for different property types. |
| **SHAP Integration** | ✅ | **Match** | backend uses `shap.TreeExplainer` for local feature attribution in the chat and valuation panels. |
| **Data Pipelines** | ✅ | **Match** | `ingest.py` and `aggregate.py` exist and function as described (merging multiple portals into PostgreSQL). |

## 2. Identified Inconsistencies (Action Required)

### A. Data Size Phrasing (Critical)
The report mentions "train boosting models on over 300,000 Singapore housing transactions" (Sections 1.2 and 2.2.2).
- **Reality:** While the cited literature [5] uses 300k records, **this project** utilizes ~25,000 live aggregated listings.
- **Fix:** Phrasing should be adjusted to show that the *methodology* is derived from large-scale studies, but applied here to a curated live dataset of 25k records.

### B. Table 5.9 Typos
The previous auto-generated table had "Condom" (a 6-character truncation unintendedly clipping 'Condominium').
- **Fix:** Use the updated Table 5.9 provided in the fixes document ($1.85M formatting and "Condo" naming).

### C. Agent Portal Scope
The report correctly identifies the "Agent Dashboard" as future work in Chapter 6, but section 4.3.4 mentions the `agent_list` table. 
- **Verification:** The `agents` and `agent_list` tables **do** exist in the DB schema to support current listing attribution, even if the GUI for agents is limited. No change needed, but good to keep consistent.

## 3. Final Recommendation
The report is technically strong and matches the implementation with high fidelity. Once the data-size phrasing is corrected and the tables are updated to the latest versions, it will be 100% accurate.
