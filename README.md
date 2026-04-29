# Distributed E-Commerce System

A scalable distributed e-commerce platform built with Flask, PostgreSQL, MongoDB, Docker, and Docker Compose. The system uses functional fragmentation across four services and simulates replication, failover, and distributed transactions.

## Architecture

### Services
- User Service: PostgreSQL primary + replica
- Order Service: PostgreSQL primary + replica
- Inventory Service: PostgreSQL primary + replica
- Product Catalog Service: MongoDB replica set
- Flask API gateway / orchestration layer (runs locally)
- Static frontend (runs locally)

### Key Capabilities
- Functional fragmentation by domain
- PostgreSQL streaming replication for each transactional service
- MongoDB replica set for product catalog data
- Transaction procedures and triggers
- Row-level locking using `SELECT FOR UPDATE`
- Two-phase commit simulation in Flask
- Admin panel for monitoring and failover simulation
- JWT-based authentication
- Seed data and indexes for performance

## Folder Structure

```text
ADBMS/
├── backend/
│   ├── app.py
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── __init__.py
│   └── shared/
│       ├── config.py
│       └── __init__.py
├── db/
│   ├── mongo/
│   │   └── init.js
│   └── postgres/
│       ├── user-db-scripts/
│       │   ├── init.sql
│       │   └── procedures.sql
│       ├── order-db-scripts/
│       │   ├── init.sql
│       │   └── procedures.sql
│       └── inventory-db-scripts/
│           ├── init.sql
│           └── procedures.sql
├── frontend/
│   ├── index.html
│   ├── login.html
│   ├── register.html
│   ├── cart.html
│   ├── product.html
│   ├── profile.html
│   ├── admin.html
│   └── assets/
│       ├── css/
│       │   └── styles.css
│       └── js/
│           ├── common.js
│           └── home.js
├── scripts/
│   └── start-databases.ps1
├── logs/
│   └── db-init/
├── docker-compose.yml
└── README.md
```

## Setup

### Prerequisites
- Docker
- Docker Compose
- Python 3.11+

### Run database containers only

```bash
docker compose up -d
```

### Run with DB setup logging (recommended)

```powershell
./scripts/start-databases.ps1
```

This writes startup and setup logs to `logs/db-init/db-startup-<timestamp>.log`.

### Run backend locally

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python app.py
```

### Run frontend locally

Use any static file server from the project root:

```powershell
python -m http.server 8080 --directory frontend
```

### Access URLs
- Frontend: `http://localhost:8080/`
- Flask API: `http://localhost:5000/health`
- PostgreSQL user service primary: `localhost:5432`
- PostgreSQL order service primary: `localhost:5434`
- PostgreSQL inventory service primary: `localhost:5436`
- MongoDB primary: `localhost:27017`

### DB setup debugging

- Check startup logs with: `docker compose logs user-db-primary order-db-primary inventory-db-primary mongodb-primary`
- Follow live logs with: `docker compose logs -f user-db-primary order-db-primary inventory-db-primary mongodb-primary`
- Use generated log files in `logs/db-init/` for post-mortem debugging.

## API Endpoints

### User Service
- `POST /api/users/register`
- `POST /api/users/login`
- `GET /api/users/profile`
- `PUT /api/users/profile`
- `GET /api/users`

### Product Service
- `GET /api/products`
- `GET /api/products/<id>`
- `POST /api/products`

### Order Service
- `POST /api/orders`
- `GET /api/orders/<order_id>`
- `GET /api/orders/user/<user_id>`

### Inventory Service
- `GET /api/inventory/<product_id>`
- `GET /api/inventory/low-stock`
- `POST /api/inventory/restock`

### Admin
- `GET /api/admin/status`
- `POST /api/admin/failover/simulate`

## Database Notes

### PostgreSQL
Each service has:
- Primary node
- Replica node
- Tables, indexes, stored procedures, triggers
- Audit-style logging

### MongoDB
- Replica set initialized with product catalog data
- Text index and category/product indexes

## Distributed Transaction Flow

1. Phase 1: Validate inventory and reserve stock.
2. Phase 2: Create the order and deduct stock.
3. On failure, rollback reservations and mark the order failed.

## Failure Simulation

The admin panel provides a mock failover trigger. For real failover orchestration, you can extend this with container runtime control or external cluster management.

## Security

- JWT authentication
- Admin-only endpoints protected by role checks
- Password hashing should be enabled before production use

## Production Notes

This project is production-style and modular, but for a real deployment you should add:
- Password hashing with bcrypt or argon2
- External secret management
- Real replica promotion automation
- Dedicated API gateway / reverse proxy hardening
- Monitoring and alerting
- Integration tests for 2PC and failover flows
