import asyncio
import logging
import time
from datetime import datetime
from typing import Dict, Any

import aiohttp
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
import pytz

from app.models import TaskSchedule, TaskExecution, CounterState, TaskType, ExecutionStatus
from app.models import AsyncSessionLocal

logger = logging.getLogger(__name__)


class TaskExecutor:
    """Handles execution of different task types"""
    
    def __init__(self):
        self.http_session = None
    
    async def start(self):
        """Initialize the executor"""
        self.http_session = aiohttp.ClientSession()
    
    async def stop(self):
        """Clean up resources"""
        if self.http_session:
            await self.http_session.close()
    
    async def execute_task(self, task_schedule: TaskSchedule) -> TaskExecution:
        """Execute a task and return the execution record"""
        async with AsyncSessionLocal() as db:
            # Create execution record
            execution = TaskExecution(
                task_id=task_schedule.id,
                status=ExecutionStatus.RUNNING,
                started_at=datetime.now(pytz.utc),
                result_data={}
            )
            
            db.add(execution)
            await db.commit()
            await db.refresh(execution)
            
            start_time = time.time()
            
            try:
                # Execute based on task type
                if task_schedule.task_type == TaskType.SLEEP:
                    result = await self._execute_sleep_task(task_schedule, db)
                elif task_schedule.task_type == TaskType.COUNTER:
                    result = await self._execute_counter_task(task_schedule, db)
                elif task_schedule.task_type == TaskType.HTTP:
                    result = await self._execute_http_task(task_schedule, db)
                else:
                    raise ValueError(f"Unknown task type: {task_schedule.task_type}")
                
                # Update execution record with success
                execution.status = ExecutionStatus.COMPLETED
                execution.completed_at = datetime.now(pytz.utc)
                execution.execution_time_ms = int((time.time() - start_time) * 1000)
                execution.result_data = result
                
                # Update task's last execution time
                task_schedule.last_executed_at = execution.completed_at
                
                logger.info(f"Task {task_schedule.name} executed successfully in {execution.execution_time_ms}ms")
                
            except Exception as e:
                # Update execution record with failure
                execution.status = ExecutionStatus.FAILED
                execution.completed_at = datetime.now(pytz.utc)
                execution.execution_time_ms = int((time.time() - start_time) * 1000)
                execution.error_message = str(e)
                
                logger.error(f"Task {task_schedule.name} failed: {str(e)}")
            
            await db.commit()
            await db.refresh(execution)
            
            return execution
    
    async def _execute_sleep_task(self, task_schedule: TaskSchedule, db: AsyncSession) -> Dict[str, Any]:
        """Execute a sleep task"""
        duration = task_schedule.task_config.get('duration_seconds', 2)
        
        sleep_start = datetime.now(pytz.utc)
        await asyncio.sleep(duration)
        sleep_end = datetime.now(pytz.utc)
        
        return {
            'duration_requested': duration,
            'duration_actual': (sleep_end - sleep_start).total_seconds(),
            'slept_at': sleep_start.isoformat(),
            'woke_at': sleep_end.isoformat()
        }
    
    async def _execute_counter_task(self, task_schedule: TaskSchedule, db: AsyncSession) -> Dict[str, Any]:
        """Execute a counter task"""
        # Get or create counter state
        result = await db.execute(
            select(CounterState).where(CounterState.task_id == task_schedule.id)
        )
        counter_state = result.scalar_one_or_none()
        
        if not counter_state:
            counter_state = CounterState(
                task_id=task_schedule.id,
                counter_value=0
            )
            db.add(counter_state)
        
        # Increment counter
        old_value = counter_state.counter_value
        counter_state.counter_value += 1
        counter_state.updated_at = datetime.now(pytz.utc)
        
        await db.commit()
        
        return {
            'previous_value': old_value,
            'new_value': counter_state.counter_value,
            'incremented_at': counter_state.updated_at.isoformat()
        }
    
    async def _execute_http_task(self, task_schedule: TaskSchedule, db: AsyncSession) -> Dict[str, Any]:
        """Execute an HTTP task"""
        url = task_schedule.task_config.get('url', 'https://httpbin.org/status/200')
        timeout = task_schedule.task_config.get('timeout_seconds', 10)
        
        request_start = time.time()
        
        try:
            async with self.http_session.get(url, timeout=timeout) as response:
                response_time_ms = int((time.time() - request_start) * 1000)
                response_body = await response.text()
                
                return {
                    'url': url,
                    'status_code': response.status,
                    'response_time_ms': response_time_ms,
                    'response_size_bytes': len(response_body),
                    'headers': dict(response.headers),
                    'success': 200 <= response.status < 300,
                    'requested_at': datetime.now(pytz.utc).isoformat()
                }
                
        except asyncio.TimeoutError:
            response_time_ms = int((time.time() - request_start) * 1000)
            return {
                'url': url,
                'status_code': None,
                'response_time_ms': response_time_ms,
                'error': 'Request timeout',
                'success': False,
                'requested_at': datetime.now(pytz.utc).isoformat()
            }