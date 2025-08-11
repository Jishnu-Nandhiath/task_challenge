"""
Pydantic schemas for request/response validation
"""
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator
from app.models import TaskType, ExecutionStatus
import pytz


class TaskScheduleBase(BaseModel):
    """Base schema for task schedules"""
    name: str = Field(..., min_length=1, max_length=255, description="Task name")
    description: Optional[str] = Field(None, description="Task description")
    task_type: TaskType = Field(..., description="Type of task to execute")
    task_config: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Task-specific configuration")


class TaskScheduleCreate(TaskScheduleBase):
    """Schema for creating a new task schedule"""
    interval_seconds: Optional[int] = Field(
        None, 
        gt=0, 
        description="Interval in seconds for recurring tasks"
    )
    scheduled_at: Optional[datetime] = Field(
        None, 
        description="Specific time for one-time execution"
    )
    
    @field_validator('scheduled_at')
    @classmethod
    def validate_scheduled_at(cls, v):
        if v and v <= datetime.now(pytz.utc):
            raise ValueError("scheduled_at must be in the future")
        return v
    
    @field_validator('task_config')
    @classmethod
    def validate_task_config(cls, v, info):
        """Validate task configuration based on task type"""
        if not info.data or 'task_type' not in info.data:
            return v
            
        task_type = info.data['task_type']
        
        if task_type == TaskType.SLEEP:
            if 'duration_seconds' not in v:
                v['duration_seconds'] = 2  # Default sleep duration
            elif not isinstance(v['duration_seconds'], (int, float)) or v['duration_seconds'] <= 0:
                raise ValueError("Sleep task requires positive duration_seconds")
                
        elif task_type == TaskType.HTTP:
            if 'url' not in v:
                v['url'] = 'https://httpbin.org/status/200'  # Default URL
            elif not isinstance(v['url'], str):
                raise ValueError("HTTP task requires valid URL string")
                
        return v
    
    @field_validator('interval_seconds')
    @classmethod
    def validate_scheduling(cls, v, info):
        """Ensure either interval_seconds or scheduled_at is provided"""
        scheduled_at = info.data.get('scheduled_at') if info.data else None
        if not v and not scheduled_at:
            raise ValueError("Either interval_seconds or scheduled_at must be provided")
        if v and scheduled_at:
            raise ValueError("Cannot specify both interval_seconds and scheduled_at")
        return v


class TaskScheduleUpdate(BaseModel):
    """Schema for updating an existing task schedule"""
    name: Optional[str] = Field(None, min_length=1, max_length=255)
    description: Optional[str] = None
    interval_seconds: Optional[int] = Field(None, gt=0)
    scheduled_at: Optional[datetime] = None
    task_config: Optional[Dict[str, Any]] = None
    is_active: Optional[bool] = None
    
    @field_validator('scheduled_at')
    @classmethod
    def validate_scheduled_at(cls, v):
        if v and v <= datetime.now(pytz.utc):
            raise ValueError("scheduled_at must be in the future")
        return v


class TaskScheduleResponse(TaskScheduleBase):
    """Schema for task schedule responses"""
    id: int
    interval_seconds: Optional[int]
    scheduled_at: Optional[datetime]
    is_active: bool
    created_at: datetime
    updated_at: datetime
    last_executed_at: Optional[datetime]
    next_execution_at: Optional[datetime]
    
    class Config:
        from_attributes = True


class TaskExecutionResponse(BaseModel):
    """Schema for task execution responses"""
    id: int
    task_id: int
    task_name: str
    task_type: TaskType
    status: ExecutionStatus
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    execution_time_ms: Optional[int]
    result_data: Optional[Dict[str, Any]]
    error_message: Optional[str]
    
    @field_validator('task_name')
    @classmethod
    def get_task_name(cls, v):
        """Extract task name from the task_schedule relationship if available"""
        return v  # This will be set in the response creation
    
    class Config:
        from_attributes = True
    
    @classmethod
    def from_execution(cls, execution):
        """Custom method to handle task_name extraction from execution"""
        data = {
            'id': execution.id,
            'task_id': execution.task_id,
            'task_name': execution.task_schedule.name if execution.task_schedule else "Unknown",
            'task_type': execution.task_schedule.task_type if execution.task_schedule else TaskType.SLEEP,
            'status': execution.status,
            'started_at': execution.started_at,
            'completed_at': execution.completed_at,
            'execution_time_ms': execution.execution_time_ms,
            'result_data': execution.result_data,
            'error_message': execution.error_message
        }
        return cls(**data)


class UpcomingExecutionResponse(BaseModel):
    """Schema for upcoming execution information"""
    task_id: int
    task_name: str
    task_type: TaskType
    scheduled_time: datetime


class HealthResponse(BaseModel):
    """Schema for health check response"""
    status: str
    scheduler: str
    timestamp: str