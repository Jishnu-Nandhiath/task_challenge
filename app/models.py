"""
Database configuration and session management
"""
from datetime import datetime
import os
import enum
from typing import AsyncGenerator, Optional

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy import Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Enum, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import pytz

Base = declarative_base()

# Database configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql+asyncpg://postgres:password@localhost:5432/task_scheduler"
)

# Create async engine
engine = create_async_engine(
    DATABASE_URL,
    echo=os.getenv("SQL_ECHO", "false").lower() == "true",
    poolclass=NullPool,  # Use NullPool for async connections
)

# Create session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

class TaskType(enum.Enum):
    """Enumeration of supported task types"""
    SLEEP = "sleep"
    COUNTER = "counter"
    HTTP = "http"


class ExecutionStatus(enum.Enum):
    """Enumeration of task execution statuses"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"



async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency to get database session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def init_db():
    """Initialize database tables"""
    async with engine.begin() as conn:
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)


async def close_db():
    """Close database engine"""
    await engine.dispose()


class TaskSchedule(Base):
    """Model for scheduled tasks"""
    __tablename__ = "task_schedules"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text, nullable=True)
    task_type = Column(Enum(TaskType), nullable=False)
    
    # Scheduling configuration
    interval_seconds = Column(Integer, nullable=True)  # For recurring tasks
    scheduled_at = Column(DateTime, nullable=True)     # For one-time tasks
    
    # Task configuration (JSON field for task-specific parameters)
    task_config = Column(JSON, nullable=True, default=dict)
    
    # Status and metadata
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now(pytz.utc), nullable=False)
    updated_at = Column(DateTime, default=datetime.now(pytz.utc), onupdate=datetime.now(pytz.utc), nullable=False)
    last_executed_at = Column(DateTime, nullable=True)
    next_execution_at = Column(DateTime, nullable=True)
    
    # Relationships
    executions = relationship("TaskExecution", back_populates="task_schedule", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<TaskSchedule(id={self.id}, name='{self.name}', type={self.task_type.value})>"


class TaskExecution(Base):
    """Model for task execution records"""
    __tablename__ = "task_executions"
    
    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("task_schedules.id"), nullable=False, index=True)
    
    # Execution details
    status = Column(Enum(ExecutionStatus), default=ExecutionStatus.PENDING, nullable=False)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    
    # Results and output
    result_data = Column(JSON, nullable=True, default=dict)  # Task-specific results
    error_message = Column(Text, nullable=True)
    
    # Performance metrics
    execution_time_ms = Column(Integer, nullable=True)  # Duration in milliseconds
    
    # Relationships
    task_schedule = relationship("TaskSchedule", back_populates="executions")
    
    @property
    def duration_seconds(self) -> Optional[float]:
        """Calculate execution duration in seconds"""
        if self.execution_time_ms is not None:
            return self.execution_time_ms / 1000.0
        return None
    
    def __repr__(self):
        return f"<TaskExecution(id={self.id}, task_id={self.task_id}, status={self.status.value})>"


class CounterState(Base):
    """Model for maintaining counter state across executions"""
    __tablename__ = "counter_states"
    
    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("task_schedules.id"), nullable=False, unique=True, index=True)
    counter_value = Column(Integer, default=0, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    def __repr__(self):
        return f"<CounterState(task_id={self.task_id}, value={self.counter_value})>"