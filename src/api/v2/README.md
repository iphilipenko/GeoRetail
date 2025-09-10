# API v2 Structure

## Domain-Driven Design Architecture

### Domains:
1. **Territories** - Discovery phase (search, hexagons, clusters)
2. **Insights** - Analysis phase (competition, potential, reports)
3. **Decisions** - Decision support (recommendations, scoring, scenarios)
4. **Portfolio** - Management (locations, contracts, projects)
5. **Admin** - System administration

### Key Directories:
- `core/` - Shared utilities and base classes
- `middleware/` - Request/response middleware
- `schemas/` - Shared Pydantic models
- `services/` - Shared business services

### Usage:
```python
from api.v2.territories.router import router as territories_router
from api.v2.insights.router import router as insights_router

app.include_router(territories_router, prefix="/api/v2/territories")
app.include_router(insights_router, prefix="/api/v2/insights")
```

### Testing:
```bash
pytest tests/v2/
```
