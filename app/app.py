"""
Task Scheduler API - Main Application
A FastAPI service for managing and executing scheduled tasks
"""
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
import pytz

from app.models import get_db, sync_engine
from app.models import TaskSchedule, TaskExecution, TaskType
from app.schema import (
    TaskScheduleCreate,
    TaskScheduleUpdate,
    TaskScheduleResponse,
    TaskExecutionResponse,
    UpcomingExecutionResponse
)
from app.scheduler import SchedulerService
from app.admin import setup_admin

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


scheduler_service = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global scheduler_service
    
    logger.info("Starting scheduler service...")
    scheduler_service = SchedulerService()
    await scheduler_service.start()
    
    yield
    
    logger.info("Stopping scheduler service...")
    if scheduler_service:
        await scheduler_service.stop()

app = FastAPI(
    title="Task Scheduler API",
    description="A REST API service for managing and executing scheduled tasks",
    version="1.0.0",
    lifespan=lifespan
)


admin = setup_admin(app, sync_engine)


@app.post("/tasks", response_model=TaskScheduleResponse)
async def create_task(
    task: TaskScheduleCreate,
    db: AsyncSession = Depends(get_db)
):
    try:
        if task.task_type not in [TaskType.SLEEP, TaskType.COUNTER, TaskType.HTTP]:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid task type. Must be one of: {[t.value for t in TaskType]}"
            )
        
        db_task = TaskSchedule(
            name=task.name,
            description=task.description,
            task_type=task.task_type,
            interval_seconds=task.interval_seconds,
            scheduled_at=task.scheduled_at,
            task_config=task.task_config or {},
            is_active=True,
        )
        
        db.add(db_task)
        await db.commit()
        await db.refresh(db_task)
        
        if db_task.is_active and scheduler_service:
            await scheduler_service.add_task(db_task)
        
        logger.info(f"Created task: {db_task.name} (ID: {db_task.id})")
        return TaskScheduleResponse.model_validate(db_task)
        
    except Exception as e:
        logger.error(f"Error creating task: {str(e)}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tasks/{task_id}", response_model=TaskScheduleResponse)
async def get_task(task_id: int, db: AsyncSession = Depends(get_db)):
    """Get information about a specific task"""
    result = await db.execute(
        select(TaskSchedule).where(TaskSchedule.id == task_id)
    )
    task = result.scalar_one_or_none()
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return TaskScheduleResponse.model_validate(task)


@app.put("/tasks/{task_id}", response_model=TaskScheduleResponse)
async def update_task(
    task_id: int,
    task_update: TaskScheduleUpdate,
    db: AsyncSession = Depends(get_db)
):
    """Update the schedule for an existing task"""
    result = await db.execute(
        select(TaskSchedule).where(TaskSchedule.id == task_id)
    )
    db_task = result.scalar_one_or_none()
    
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    try:
        if task_update.name is not None:
            db_task.name = task_update.name
        if task_update.description is not None:
            db_task.description = task_update.description
        if task_update.interval_seconds is not None:
            db_task.interval_seconds = task_update.interval_seconds
        if task_update.scheduled_at is not None:
            db_task.scheduled_at = task_update.scheduled_at
        if task_update.task_config is not None:
            db_task.task_config = task_update.task_config
        if task_update.is_active is not None:
            db_task.is_active = task_update.is_active
            
        db_task.updated_at = datetime.now(pytz.utc)
        
        await db.commit()
        await db.refresh(db_task)
        
        if scheduler_service:
            if db_task.is_active:
                await scheduler_service.update_task(db_task)
            else:
                await scheduler_service.remove_task(task_id)
        
        logger.info(f"Updated task: {db_task.name} (ID: {db_task.id})")
        return TaskScheduleResponse.model_validate(db_task)
        
    except Exception as e:
        logger.error(f"Error updating task {task_id}: {str(e)}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/tasks/{task_id}")
async def delete_task(task_id: int, db: AsyncSession = Depends(get_db)):
    """Delete a task"""
    result = await db.execute(
        select(TaskSchedule).where(TaskSchedule.id == task_id)
    )
    db_task = result.scalar_one_or_none()
    
    if not db_task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    try:
        if scheduler_service:
            await scheduler_service.remove_task(task_id)
        
        await db.delete(db_task)
        await db.commit()
        
        logger.info(f"Deleted task: {db_task.name} (ID: {db_task.id})")
        return {"message": "Task deleted successfully"}
        
    except Exception as e:
        logger.error(f"Error deleting task {task_id}: {str(e)}")
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/tasks", response_model=List[TaskScheduleResponse])
async def list_tasks(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = False,
    db: AsyncSession = Depends(get_db)
):
    """List all tasks with optional filtering"""
    query = select(TaskSchedule).offset(skip).limit(limit)
    
    if active_only:
        query = query.where(TaskSchedule.is_active)
    
    result = await db.execute(query)
    tasks = result.scalars().all()
    
    return [TaskScheduleResponse.model_validate(task) for task in tasks]


@app.get("/tasks/{task_id}/upcoming", response_model=List[UpcomingExecutionResponse])
async def get_upcoming_executions(
    task_id: int,
    count: int = 5,
    db: AsyncSession = Depends(get_db)
):
    """Get upcoming executions for a specific task"""
    result = await db.execute(
        select(TaskSchedule).where(TaskSchedule.id == task_id)
    )
    task = result.scalar_one_or_none()
    
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    
    if not task.is_active:
        return []
    
    upcoming = []
    current_time = datetime.now(pytz.utc)
    
    if task.scheduled_at:
        if task.scheduled_at > current_time:
            upcoming.append(UpcomingExecutionResponse(
                task_id=task.id,
                task_name=task.name,
                scheduled_time=task.scheduled_at,
                task_type=task.task_type
            ))
    elif task.interval_seconds:
        next_time = current_time
        for _ in range(count):
            next_time = next_time + timedelta(seconds=task.interval_seconds)
            upcoming.append(UpcomingExecutionResponse(
                task_id=task.id,
                task_name=task.name,
                scheduled_time=next_time,
                task_type=task.task_type
            ))
    
    return upcoming


@app.get("/executions", response_model=List[TaskExecutionResponse])
async def list_executions(
    task_id: Optional[int] = None,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db)
):
    """List all past task executions with details"""
    query = select(TaskExecution).options(
        selectinload(TaskExecution.task_schedule)
    ).order_by(TaskExecution.started_at.desc()).offset(skip).limit(limit)
    
    if task_id:
        query = query.where(TaskExecution.task_id == task_id)
    
    result = await db.execute(query)
    executions = result.scalars().all()
    
    return [TaskExecutionResponse.from_execution(execution) for execution in executions]


@app.get("/executions/{execution_id}", response_model=TaskExecutionResponse)
async def get_execution(execution_id: int, db: AsyncSession = Depends(get_db)):
    """Get details about a specific task execution"""
    result = await db.execute(
        select(TaskExecution)
        .options(selectinload(TaskExecution.task_schedule))
        .where(TaskExecution.id == execution_id)
    )
    execution = result.scalar_one_or_none()
    
    if not execution:
        raise HTTPException(status_code=404, detail="Execution not found")
    
    return TaskExecutionResponse.from_execution(execution)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    scheduler_status = "running" if scheduler_service and scheduler_service.is_running else "stopped"
    return {
        "status": "healthy",
        "scheduler": scheduler_status,
        "timestamp": datetime.now(pytz.utc).isoformat()
    }