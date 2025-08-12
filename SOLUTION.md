## Architecture & Design Decisions

### Core Architecture

**1. FastAPI Application (`app/app.py`)**
- RESTful API with proper HTTP status codes and error handling
- Async/await throughout for optimal performance

**2. Custom Async Scheduler (`app/scheduler.py`)**
- Single-process, async-based scheduler inspired by Celery
- Min-heap data structure for efficient task scheduling
- Non-overlapping execution guarantee per task instance

**3. Task Execution Engine (`app/tasks.py`)**
- Pluggable task executor supporting multiple task types
- Comprehensive execution tracking with timing metrics
- Error handling and recovery mechanisms


### Key Design Decisions

**Scheduler Architecture**: Chose a custom async scheduler over Celery/Redis because:
- Simpler deployment with fewer moving parts
- Better integration with FastAPI's async ecosystem
- No external message broker dependency

**Database Design**: 
- Separate `TaskExecution` table for detailed execution history
- `CounterState` table for persistent counter values
    - Could've used redis for counterstate, but didn't want to use it just for this
- `next_execution_at` field for efficient scheduling queries
- JSON fields for custom task configuration

**Concurrency Model**:
- Async/await throughout the application
- Configurable concurrent task execution limit
- No self-overlap guarantee using running task tracking
- Event-driven scheduler wake-up for responsiveness

## API Endpoints

The API provides comprehensive task management capabilities:

- `POST /tasks` - Create a new scheduled task
- `GET /tasks/{id}` - Retrieve specific task information
- `PUT /tasks/{id}` - Update task schedule or configuration
- `DELETE /tasks/{id}` - Remove a task
- `GET /tasks` - List all tasks with filtering
- `GET /tasks/{id}/upcoming` - View upcoming executions
- `GET /executions` - List execution history
- `GET /executions/{id}` - Get specific execution details
- `GET /health` - Health check endpoint
- `/admin/` - Administrative web interface

## Setup and Deployment Instructions

### Prerequisites
- Docker and Docker Compose
- Python 3.10+ (for local development)
- PostgreSQL 15+ (if running locally)


### Docker Deployment

```bash
docker-compose up -d
# wait for some time and run
docker-compose exec app uv run alembic upgrade head
```

This will:
- Start PostgreSQL database with proper initialization
- Build and run the FastAPI application
- Set up networking between services
- Configure health checks for both services

**Access Points:**
- API: http://localhost:8000
- API Documentation: http://localhost:8000/docs
- Admin Interface: http://localhost:8000/admin
- Health Check: http://localhost:8000/health

## Trade-offs and Assumptions

### Trade-offs Made

1. **Single-Process Scheduler vs. Distributed System:**
   - **Chosen**: Single-process async scheduler, tasks are executed in FIFO manner
   - **Trade-off**: Simpler deployment but no horizontal scaling

2. **Custom Scheduler vs. Celery:**
   - **Chosen**: Custom async scheduler
   - **Trade-off**: More implementation work but tighter integration

3. **In-Memory Task Queue vs. Database Queue:**
   - **Chosen**: In-memory with database persistence
   - **Trade-off**: Faster execution but requires restart coordination


### Assumptions and Tradeoffs

1. **Task Execution**: Tasks are reasonably lightweight (< 30 seconds). 
2. **Error Recovery**: Failed tasks don't require automatic retry (though this could be added)
3. **Scalability**: Single-instance deployment is sufficient for initial requirements
4. **No Priority**: Tasks are executed in FIFO, no way to jump over for new tasks. 
5. **Scheduled Tasks may not get executed at the exact time**

### Specific AI Assistance Areas

1. **Architecture Planning:**
   - Prompt: Write an implementation of celery using python asyncio, that can handle recurring and scheduled tasks. The tasks can be added in between using API. Do not use broker, prefer in-memory for storing queue.
   - The core design was around this, with some hours of debugging and reading
2. **Basic DB Schema Design:**
3. **Error Handling Patterns:**
4. **Docker Configuration:**


## Additional Features Implemented

### Bonus Features Achieved

1. **Graceful Shutdown**: Application properly waits for running tasks to complete on shutdown
2. **Comprehensive Logging**: Structured logging throughout the application with appropriate levels
3. **Admin Interface**: Web-based admin panel for task management using SQLAdmin
4. **Health Checks**: Both application and database health monitoring
5. **Performance Considerations**: Async throughout, efficient database queries, connection pooling

## Performance Characteristics

- **Concurrent Tasks**: Configurable limit (default: 10)
- **Database Connections**: Async connection pooling with SQLAlchemy
- **Memory Usage**: Minimal task queue overhead, scales with active task count