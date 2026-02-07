# Backend API

FastAPI-based backend for the Singapore Real Estate Data Platform.

## Tech Stack

- **Framework**: FastAPI
- **Database**: PostgreSQL (async via asyncpg)
- **Cache**: Redis
- **ORM**: SQLAlchemy 2.0
- **Auth**: JWT (python-jose)

## Quick Start

### Using Docker (Recommended)

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f backend

# Stop services
docker-compose down
```

**Services:**
| Service | Port | Description |
|---------|------|-------------|
| backend | 8000 | FastAPI application |
| db | 5432 | PostgreSQL database |
| redis | 6379 | Redis cache |

### Local Development

```bash
# Start only database services
docker-compose up -d db redis

# Install dependencies
pip install -r requirements.txt

# Run with hot reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## Project Structure

```
backend/
├── app/
│   ├── main.py           # FastAPI application entry
│   ├── config.py         # Settings and configuration
│   ├── database.py       # Database connection setup
│   ├── models/           # SQLAlchemy models
│   │   ├── user.py
│   │   ├── agent.py
│   │   └── listing.py
│   ├── routers/          # API route handlers
│   │   ├── auth.py
│   │   ├── listings.py
│   │   └── agents.py
│   ├── schemas/          # Pydantic schemas
│   ├── services/         # Business logic
│   └── utils/            # Utility functions
├── docker-compose.yml    # Docker services config
├── Dockerfile            # Container build
├── requirements.txt      # Python dependencies
├── seed_db.py            # Sample data seeder
└── README.md             # This file
```

## Environment Variables

| Variable     | Default           | Description       |
| ------------ | ----------------- | ----------------- |
| `DB_HOST`    | `db`              | PostgreSQL host   |
| `DB_PORT`    | `5432`            | PostgreSQL port   |
| `DB_USER`    | `postgres`        | Database user     |
| `DB_PASS`    | `postgres`        | Database password |
| `DB_NAME`    | `real_estate_app` | Database name     |
| `REDIS_HOST` | `redis`           | Redis host        |
| `REDIS_PORT` | `6379`            | Redis port        |
| `SECRET_KEY` | -                 | JWT secret key    |

## Database Management

```bash
# Seed sample data
python seed_db.py

# Reset database
python reset_db.py

# Initialize tables
python init_db.py
```

## API Endpoints Overview

### Authentication

- `POST /auth/register` - Register new user
- `POST /auth/login` - Login and get JWT token
- `GET /auth/me` - Get current user info

### Listings

- `GET /listings` - List all properties (with filters)
- `GET /listings/{id}` - Get listing details
- `POST /listings` - Create listing (agent only)
- `PUT /listings/{id}` - Update listing
- `DELETE /listings/{id}` - Delete listing

### Agents

- `GET /agents` - List agents
- `GET /agents/{id}` - Get agent details with listings
