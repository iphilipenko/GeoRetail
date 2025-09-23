# UC Architecture Documentation
## Detailed Use Case-Driven Architecture for GeoRetail v2

### Architecture Philosophy

The UC-driven architecture organizes code around user workflows rather than technical domains. 
This approach aligns development with actual user needs and usage patterns.

### Use Case Definitions

#### UC1: Explorer Mode
**Purpose:** Visual exploration and discovery of territories
**Primary Users:** Marketing analysts, expansion managers
**Key Features:**
- Interactive map with multiple data layers
- Drill-down navigation from country to H3-10
- Real-time metric calculations
- Bivariate choropleth visualization

#### UC2: Screening Mode  
**Purpose:** Batch assessment of multiple locations
**Primary Users:** Expansion teams, data analysts
**Key Features:**
- Batch scoring of 100+ locations
- ML-powered predictions
- Heatmap generation
- Automated filtering and ranking

#### UC3: Comparison Mode
**Purpose:** Detailed comparison of finalist locations
**Primary Users:** Decision makers, executives
**Key Features:**
- Side-by-side comparison
- Spider chart visualization
- Cannibalization analysis
- ROI forecasting

### Technical Architecture

#### Layered Structure
```
Presentation Layer (Endpoints)
    ↓
Business Logic Layer (Services)
    ↓
Data Access Layer (Database/Cache)
```

#### Service Communication
- Services are independent and communicate through well-defined interfaces
- Shared components in `shared/` directory provide common functionality
- Each UC has its own schema definitions to maintain independence

#### Caching Strategy
- Redis for real-time data (TTL: 5 minutes)
- Pre-calculated metrics in ClickHouse
- Client-side caching for static data

#### Security Model
- JWT-based authentication
- RBAC permission system
- UC-specific permission checks
- Audit logging for all operations

### Performance Targets

| Use Case | Response Time | Concurrent Users |
|----------|---------------|------------------|
| Explorer | < 200ms | 100 |
| Screening | < 5s (batch) | 20 |
| Comparison | < 500ms | 50 |

### Scalability Considerations

1. **Horizontal Scaling:** Each UC can be deployed independently
2. **Database Sharding:** H3 data partitioned by resolution
3. **Async Processing:** Celery for batch operations
4. **CDN:** Static assets and map tiles

### Monitoring and Observability

- Prometheus metrics for performance
- ELK stack for logging
- Sentry for error tracking
- Custom dashboards for business metrics

### Development Workflow

1. **Feature Development:**
   - Create feature branch from develop
   - Implement in relevant UC module
   - Write tests (minimum 80% coverage)
   - Create/update API documentation

2. **Code Review:**
   - PR requires 2 approvals
   - Automated tests must pass
   - Performance benchmarks checked

3. **Deployment:**
   - Staging environment first
   - Smoke tests
   - Gradual rollout to production

### API Versioning Strategy

- Current version: v2
- Deprecation notice: 3 months
- Backward compatibility: 6 months
- Version sunset: 12 months

### Dependencies

#### External Services
- PostgreSQL with PostGIS
- ClickHouse for analytics
- Redis for caching
- Celery + RabbitMQ for async tasks

#### Python Packages
- FastAPI >= 0.104.0
- SQLAlchemy >= 2.0.0
- Pydantic >= 2.0.0
- Redis >= 5.0.0
- Celery >= 5.3.0

### Future Roadmap

#### Q1 2025
- Complete UC implementation
- Performance optimization
- Enhanced caching

#### Q2 2025
- GraphQL API addition
- Real-time WebSocket updates
- Advanced ML models

#### Q3 2025
- Mobile API optimization
- Offline mode support
- International expansion

### Contact

- Technical Lead: tech-lead@georetail.com
- API Support: api-support@georetail.com
- Documentation: docs@georetail.com
