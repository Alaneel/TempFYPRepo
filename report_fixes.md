# FYP Final Report: Suggested Text Fixes

Please apply the following changes to your **FYP_Final_report-6.docx** to ensure it is 100% accurate to the implementation.

---

## 1. Data Count Citation Correction
**Location:** Section 1.2 (Motivation) and Section 2.2.2 (Data Sources)

**Current Text (approx):**
> "...train boosting models on over 300,000 Singapore housing transactions [5]"

**Revised Text:**
> "...apply advanced gradient boosting methodologies—proven effective on large-scale datasets of 300,000 records in literature [5]—to a curated real-time dataset of **over 25,000 aggregated listings** collected across multiple Singapore property portals for this platform."

---

## 2. Updated Table 5.9 (Final Version)
**Location:** Section 5.5.4 (Summary Findings)

Replace your existing Table 5.9 with this one. It fixes the "Condom" typo and uses the requested price formatting (e.g., $1.85M).

| Profile | NDCG@5 | Top-1 Result | Price Match | Type | Beds |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **User A** | 0.500 | SIMSVILLE ($1.85M) | Within 8% | Condo | 3BR |
| **User B** | 1.000 | Jalan Raja Udang ($8.29M) | Within 4% | Landed | 5BR |
| **User C** | 1.000 | Daintree Residence ($4,600) | Within 2% | Condo | 2BR |

---

## 3. Section 5.5.4 Closing Sentence
**Location:** End of Section 5.5.4

**Add this sentence:**
> "The high NDCG@5 scores for Users B and C (1.000) demonstrate the engine's ability to perfectly rank relevant properties when preference signals are strong, while the score of 0.500 for User A reflects the systemic challenge of ranking highly similar condominium units within the same district where price variances are minimal."

---

## 4. Chapter 5 Table Verification
Ensure all tables in your report match the auto-generated versions in the `chapter5_evaluation_tables.md` file I have provided. Specifically check **Table 5.4** for the MAPE values (18.0% and 18.1%).

---

---

## 5. Role-Based System Participation
**Location:** Section 3.2 (System Architecture) or 3.3 (User Entities)

**Revised Text:**
> "The system distinguishes between two primary user personas to ensure data integrity and utility. **Homeowners (Customers)** utilize the platform to browse the comprehensive property directory and identify their next home using the hybrid recommendation engine. **Verified Agents**, once authenticated and linked to their CEA numbers via the `agent_list` reference table, are granted elevated privileges to contribute directly to the master directory. This includes the ability to update specific unit attributes—such as direction facing and afternoon sun exposure—during physical viewings, thereby enriching the global property directory for all users."

---

## 6. Future Works: Crowdsourced Directory Enrichment
**Location:** Chapter 6 (Conclusion & Future Works)

**Add this paragraph:**
> "Future iterations of the system will formalize the 'Contributor' workflow, where data entries provided by verified agents into the `hdb_unit` and `condo_unit` tables are weight-verified against multiple submissions. This crowdsourcing mechanism will transform the master directory from a static repository into a dynamic, community-updated 'Wikipedia' for Singapore real estate, effectively solving the industry-wide challenge of missing granular unit-level metadata."

---

---

## 7. Database Design: Reference Tables
**Location:** Section 4.3.3 (Reference Tables)

**Revised Text:**
> "The reference tables provide a stable foundation for the property directory by separating permanent physical attributes from dynamic market activity. The master property directories for condominiums and public housing blocks serve as the primary repositories for building-level metadata. Beneath these, the master unit tables store granular, unit-level information—such as floor level and orientation—that remains permanent even as individual sales listings change. Supporting tables for users and agents facilitate personalization and track professional relationships, while an authoritative registry of licensed practitioners is used to verify agent credentials before they are permitted to contribute data to the master directory. Together, these tables create a balanced schema that ensures long-term data persistence beyond the lifecycle of any single advertisement."

---

## 8. Marketplace Listing Implementation
**Location:** Section 4.2 (Marketplace Listing Implementation)

**Revised Text:**
> "The listings table is the primary repository for all marketplace data. Rather than splitting listings into separate HDB and condominium tables, the system consolidates all observations into a single, denormalized structure. This approach was chosen because recommendation and valuation services require high comparability across diverse property types, and a unified table significantly simplifies the underlying service logic. Each listing record captures real-time data—including price, floor area, and bedroom count—while maintaining direct links to both the building directory and the specific unit master record. This hybrid design allows the application to treat listings as the primary point of user interaction while ensuring that every advertisement is enriched with stable, verified metadata from the master property directory."

---

---

## 9. Recommendation Pipeline Design
**Location:** Section 3.9 (Recommendation Engine)

**Revised Text:**
> "The recommendation pipeline follows a two-stage retrieval-and-ranking design. Candidate listings are first retrieved from the database using broad filtering conditions, such as property type and a specific price band relative to the user's historical preferences. This initial stage significantly reduces the candidate pool before detailed scoring is performed. The remaining candidates are then ranked using a hybrid scoring function that combines six dimensions: property type frequency, district relevance, price similarity, bedroom-count alignment, a valuation-grounded bargain signal, and specific condominium facilities. This design allows recommendations to reflect both attribute-level preference and model-informed value attractiveness, providing a personalized and value-aware ranking for every user."

---

## 10. Bargain Score Formulation
**Location:** Section 3.9.2 (Bargain Score Formulation)

**Revised Text:**
> "The bargain score is generated through a specialized function that quantifies the degree to which a listing appears underpriced relative to the machine learning model's predicted fair value. The raw bargain value is computed as the percentage difference between the predicted estimate and the actual listing price. This raw score is then clamped to a fixed range and linearly shifted to a scale of 0.0 to 1.0, where 0.5 represents a listing priced exactly at the model's estimate. A neutral score is assigned when insufficient data is available for valuation inference. This normalized bargain score is weighted at 15% in the final composite recommendation ranking, ensuring that value-for-money is a core driver of property relevance."

---

---

## 12. Backend Implementation: ETL and Ingestion Pipelines
**Location:** Section 4.4 and 4.4.1 (ETL and Data Ingestion Pipelines)

**Revised Text:**
> "The backend implementation utilizes a two-stage ETL pipeline to collect, clean, and enrich housing data. In the first stage, a dedicated aggregation script processes raw scraped files from multiple portals to normalize columns and remove duplicate entries using composite deduplication keys. The resulting cleaned dataset is stored in an intermediate SQLite database, which isolates the deduplication logic from the main application and allows for data inspection before final ingestion. In the second stage, an ingestion script reads from this intermediate source and loads the records into the PostgreSQL schema. During this process, each listing is matched against the master property directory for condominiums and public housing using deterministic text-matching rules. Furthermore, the pipeline extracts granular unit identifiers (e.g., '#12-345') from listing text to automatically link marketplace events to specific master unit records, ensuring that real-world observations such as price and size are used to continuously enrich the permanent property directory."

---

---

## 13. Data Cleaning and Consolidation Logic
**Location:** Section 4.4.2 (Data Cleaning and Consolidation Logic)

**Revised Text:**
> "Data cleaning ensures that raw marketplace records are standardized for analysis. The logic removes incomplete entries, resolves inconsistent categorical values, and converts textual price and area fields into structured numeric forms. By applying these standardization functions before matching, the system improves relational consistency and ensures that listing data is ready for unit-level linkage and valuation modelling."

---

## 14. Property Matching and Deduplication Implementation
**Location:** Section 4.4.3 (Property Matching and Deduplication Implementation)

**Revised Text:**
> "The deduplication and matching pipeline operates in two phases. First, cross-portal duplicates are removed by constructing composite keys from normalized addresses, room counts, and floor areas. PropertyGuru records are prioritized during this process. Second, the ingestion script links these deduplicated listings to the master directory using case-insensitive title matches for condominiums and block-pattern matching for public housing. Crucially, specific unit identifiers are extracted from listing text to establish linkages to the master unit directory, ensuring every advertisement is enriched with permanent physical metadata."

---

---

## 15. Valuation Service Implementation
**Location:** Section 4.4.4 (Valuation Service Implementation)

**Revised Text:**
> "The valuation service is a backend module that generates feature vectors by combining dynamic marketplace attributes with stable directory metadata. These vectors are processed by a trained XGBoost model to estimate fair market value in real time. Beyond price prediction, the service provides the quantitative basis for bargain-aware ranking; listings priced below the model's estimate are prioritized by the recommendation engine. The service is exposed via RESTful endpoints, allowing the frontend to deliver instant valuation insights for any selected property."

---

## 16. Recommendation Service Implementation
**Location:** Section 4.4.5 (Recommendation Service Implementation)

**Revised Text:**
> "The recommendation service implements a hybrid ranking logic that evaluates listings across multiple dimensions, including location, price similarity, and value-for-money. The implementation follows a two-stage design: broad candidate retrieval followed by a weighted aggregation of preference scores. By incorporating model-derived valuation data, the engine moves beyond simple filtering to prioritize listings that are statistically underpriced relative to user preferences. This architectural decision transforms the system from a passive directory into an active decision-support platform."

---

## 17. Explainability Service Implementation
**Location:** Section 4.4.6 (Explainability Service Implementation)

**Revised Text:**
> "The explainability service utilizes SHAP (SHapley Additive exPlanations) to decompose valuation outputs into interpretable feature attributions. By applying the TreeExplainer algorithm to tree-based models, the backend identifies the relative impact of attributes such as floor area and location on the predicted price. Decoupling this service from the core prediction logic ensures modularity and allows for reusable explanation workflows. Attribution data is delivered as a structured JSON payload, enabling the frontend to render the model's decision-making process into intuitive, color-coded visual insights for the user."

---

**Status:** ALL Technical Features & Documentation Verified.
