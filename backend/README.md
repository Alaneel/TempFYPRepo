# Backend API

FastAPI-based backend for the Singapore Real Estate Data Platform.

## Tech Stack

- **Framework**: FastAPI
- **Database**: PostgreSQL (via SQLAlchemy)
- **Cache**: Redis
- **Auth**: JWT (python-jose)
- **AI**: Claude (Anthropic) for semantic search, XGBoost/RF for valuation

## Quick Start

### Using Docker (Recommended)

```bash
docker-compose up -d        # Start all services
docker-compose logs -f backend
docker-compose down
```

### Local Development

```bash
docker-compose up -d db redis   # Just DB + Redis

# Copy and edit env file
cp ../.env.example ../.env

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Project Structure

```
backend/
├── app/
│   ├── main.py             # FastAPI application + router registration
│   ├── config.py           # Settings loaded from .env
│   ├── database.py         # DB connection
│   ├── models/             # SQLAlchemy ORM models
│   │   ├── listing.py
│   │   ├── agent.py
│   │   └── user.py
│   ├── routers/            # Route handlers
│   │   ├── listings.py     # GET /listings, /listings/{id}, POST semantic-search
│   │   ├── valuation.py    # POST /valuation/estimate, GET /valuation/health
│   │   ├── agents.py
│   │   ├── auth.py
│   │   └── admin.py
│   ├── services/           # Business logic
│   │   └── valuation.py    # Model loader, feature engineering, SHAP attribution
│   └── schemas/            # Pydantic request/response schemas
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Environment Variables

| Variable            | Default           | Description                      |
| ------------------- | ----------------- | -------------------------------- |
| `DB_HOST`           | `db`              | PostgreSQL host                  |
| `DB_PORT`           | `5432`            | PostgreSQL port                  |
| `DB_USER`           | `postgres`        | Database user                    |
| `DB_PASS`           | `postgres`        | Database password                |
| `DB_NAME`           | `real_estate_app` | Database name                    |
| `REDIS_HOST`        | `redis`           | Redis host                       |
| `SECRET_KEY`        | —                 | JWT signing secret               |
| `ANTHROPIC_API_KEY` | —                 | Claude API key (semantic search) |

## API Endpoints

### Listings

| Method | Endpoint                           | Description                                                     |
| ------ | ---------------------------------- | --------------------------------------------------------------- |
| `GET`  | `/api/v1/listings`                 | List properties with filters (q, property_type, buy_rent, page) |
| `GET`  | `/api/v1/listings/{id}`            | Full listing detail with agent + condo info                     |
| `POST` | `/api/v1/listings/semantic-search` | Claude-powered natural language search                          |

### Valuation

| Method | Endpoint                     | Description                                            |
| ------ | ---------------------------- | ------------------------------------------------------ |
| `POST` | `/api/v1/valuation/estimate` | AI price estimate with confidence range + SHAP factors |
| `GET`  | `/api/v1/valuation/health`   | Model availability status                              |

**Estimate request body:**

```json
{
  "property_type": "Condominium",
  "buy_rent": "property-for-sale",
  "beds": 3,
  "sqft": 1200,
  "tenure": "Freehold",
  "actual_price": 1500000
}
```

**Estimate response:**

```json
{
  "estimate": 1280000,
  "range_low": 998400,
  "range_high": 1561600,
  "mode": "sale",
  "segment": "condo_sale",
  "mape": 0.22,
  "premium_pct": 17.2,
  "verdict": "overpriced",
  "shap_factors": [
    { "label": "Freehold tenure", "impact_sgd": 120000, "direction": "positive" },
    ...
  ]
}
```

> [!NOTE]
> Valuation models must be trained before the estimate endpoint is available.
> Run `python pipeline/valuation_model.py --quick` from the project root.

### Agents

| Method | Endpoint              | Description                |
| ------ | --------------------- | -------------------------- |
| `GET`  | `/api/v1/agents`      | List agents                |
| `GET`  | `/api/v1/agents/{id}` | Agent detail with listings |

### Auth

| Method | Endpoint                | Description        |
| ------ | ----------------------- | ------------------ |
| `POST` | `/api/v1/auth/register` | Register new user  |
| `POST` | `/api/v1/auth/login`    | Login, returns JWT |
| `GET`  | `/api/v1/auth/me`       | Current user info  |
