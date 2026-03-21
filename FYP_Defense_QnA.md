# FYP Defense Q&A: Potential Examiner Questions

### **1. Architecture & Infrastructure**

**Question: Why did you choose to Dockerize the application?**
- **Answer:** Docker was essential for three reasons:
    - **Environment Parity:** ML libraries (like XGBoost, LightGBM, SHAP) have complex C++ dependencies. Docker ensures the models run identically on my machine and the examiner's machine.
    - **Microservices Orchestration:** The project uses a Next.js frontend, a FastAPI backend, and a PostgreSQL database. Docker Compose manages these as a single unit.
    - **CI/CD Readiness:** Containerization follows industry best practices for scalable, cloud-native deployments.

**Question: Why use a Microservices architecture (FastAPI + Next.js) instead of a single monolith (like Django or Next.js API)?**
- **Answer:** 
    - **Language Specialization:** Next.js is world-class for UI/UX, while Python (FastAPI) is the standard for Data Science and ML. 
    - **Independent Scaling:** In a production environment, the heavy AI inference (Valuation) can be scaled separately from the lightweight frontend.

---

### **2. Data Engineering & Integrity**

**Question: How do you handle duplicate listings from different websites (PropertyGuru vs. 99.co)?**
- **Answer:** We implemented a custom **Composite-Key Deduplication Engine**.
- **The Logic:** We normalize the Address, Bedroom Count, and Square Footage into a single string (e.g., `123orchardrd_3_1200`). If two listings from different portals have the same key, we collapse them into one "Canonical Record" to prevent market inflation.

**Question: Why is some data (like TOP year) missing in the Property Directory?**
- **Answer (The Scraper Audit):** This was a conscious trade-off between **Throughput** and **Depth**.
- **The Explanation:** Our scraper targets "Search Grid Cards" for high speed, allowing us to index 25,000+ listings. To get 100% completeness on every secondary attribute, we would need to crawl "Detail Pages," which is 10x slower and risks IP blocking. We prioritized having a wide market overview.

---

### **3. Machine Learning & Recommendations**

**Question: How did you evaluate the Recommendation Engine without real users?**
- **Answer:** I designed a **Synthetic User Evaluation Suite**.
- **The Process:** I programmatically created "Fake Buyer Personas" (e.g., Budget Renter, East Coast Family) with specific constraints. I then used my `eval_hybrid_recs.py` script to calculate **NDCG (Normalized Discounted Cumulative Gain)** to see if the AI's top-5 results actually matched the persona's needs.

**Question: Why move from MCDM (Multi-Criteria Decision Making) to a Hybrid Engine?**
- **Answer:** Initially, we tried MCDM, but it required users to fill out annoying surveys (Pairwise Ranking). We pivoted to a **Hybrid Content-Based + Valuation Engine** because it learns from "Implicit Behavior" (what you click/save), which is much more user-friendly and realistic.

---

### **4. Security & Performance**

**Question: Is the interactive mortgage calculator secure? Can users manipulate the data?**
- **Answer:** The Mortgage Calculator is a **purely client-side utility**. It does not write to the database or affect the AI models. It is designed for UX speed (zero latency), and because it is an "estimation tool," it does not require server-side validation.

**Question: How does the app handle 25,000+ listings without lagging?**
- **Answer:** We use **Two-Tiered Caching**:
    - **Backend:** Models are loaded into RAM once at startup for 5ms inference.
    - **Database:** PostgreSQL is indexed on `district` and `property_type` for sub-second retrieval.
