# Migration Guide: DDD → UC Architecture

## Overview
This guide helps migrate from the old Domain-Driven Design to the new Use Case-driven architecture.

## Mapping Old to New

| Old DDD Endpoint | New UC Endpoint | Notes |
|-----------------|-----------------|-------|
| /territories/admin/geometries/all | /explorer/map/initial_load | Combined with metrics |
| /territories/h3/grid | /explorer/layers/hexagons | Zoom-based filtering |
| /territories/poi/search | /explorer/layers/poi | Viewport optimized |
| /territories/bivariate/config | /explorer/metrics/bivariate | Same logic |

## Migration Steps

### Phase 1: Aliasing (Day 1)
1. Create aliases for existing endpoints
2. No logic changes
3. Both old and new URLs work

### Phase 2: Refactoring (Week 1)
1. Move logic to UC services
2. Optimize for use cases
3. Add caching

### Phase 3: Deprecation (Month 1)
1. Add deprecation warnings
2. Update documentation
3. Notify frontend team

### Phase 4: Removal (Month 3)
1. Remove old endpoints
2. Clean up code
3. Final testing

## Code Examples

### Old Way (DDD)
```python
@router.get("/territories/admin/{id}")
async def get_territory(id: str):
    return territory_service.get_by_id(id)
```

### New Way (UC)
```python
@router.get("/explorer/details/territory/{id}")
async def get_territory_details(
    id: str,
    include_children: bool = False,
    include_metrics: bool = True
):
    return explorer_service.get_territory_with_context(
        id, include_children, include_metrics
    )
```

## Testing
Run both old and new endpoints in parallel:
```bash
# Test old endpoints
pytest tests/test_territories/

# Test new endpoints  
pytest src/api/v2/tests/
```
