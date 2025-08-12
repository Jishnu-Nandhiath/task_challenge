"""
Task Scheduler Service
A Celery-inspired async task scheduler for managing and executing scheduled tasks
"""
import asyncio
import heapq
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, Set, Optional, Any

from sqlalchemy.future import select
import pytz

from app.models import TaskSchedule, AsyncSessionLocal
from app.tasks import TaskExecutor


logger = logging.getLogger(__name__)

@dataclass
class ScheduledTask:
    """Represents a task scheduled for execution"""
    task_id: int
    execute_at: datetime
    task_schedule: TaskSchedule
    is_recurring: bool = False
    execution_id: Optional[str] = None
    
    def __lt__(self, other):
        """Compare tasks by execution time for heap ordering"""
        return self.execute_at < other.execute_at
    
    def __eq__(self, other):
        """Tasks are equal if they have the same task_id"""
        return isinstance(other, ScheduledTask) and self.task_id == other.task_id


class SchedulerService:
    """
    Async task scheduler inspired by Celery but designed for single-process async operation.
    Manages task scheduling, execution, and lifecycle.
    """
    
    def __init__(self):
        self.task_queue: list = []  # Min-heap of ScheduledTask objects
        self.active_tasks: Dict[int, ScheduledTask] = {}  # task_id -> ScheduledTask
        self.running_tasks: Set[int] = set()  # task_ids currently executing
        self.executor = TaskExecutor()
        
        # Control variables
        self.scheduler_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.new_task_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()
        
        # Configuration
        self.max_concurrent_tasks = 10
        self.scheduler_tick_interval = 1  # seconds
        
        logger.info("SchedulerService initialized")
    
    async def start(self):
        """Start the scheduler service"""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        logger.info("Starting SchedulerService...")
        
        await self.executor.start()
        
        await self._load_active_tasks()
        
        # Start the main scheduler loop
        self.is_running = True
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
        
        logger.info(f"SchedulerService started with {len(self.active_tasks)} active tasks")
    
    async def stop(self):
        """Stop the scheduler service gracefully"""
        if not self.is_running:
            return
        
        logger.info("Stopping SchedulerService...")
        
        # Signal shutdown
        self.is_running = False
        self.shutdown_event.set()
        
        # Cancel the scheduler loop
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        # Wait for running tasks to complete (with timeout)
        if self.running_tasks:
            logger.info(f"Waiting for {len(self.running_tasks)} running tasks to complete...")
            # Give tasks 30 seconds to complete gracefully
            for _ in range(30):
                if not self.running_tasks:
                    break
                await asyncio.sleep(1)
            
            if self.running_tasks:
                logger.warning(f"Force stopping with {len(self.running_tasks)} tasks still running")
        
        # Clean up executor
        await self.executor.stop()
        
        logger.info("SchedulerService stopped")
    
    async def add_task(self, task_schedule: TaskSchedule):
        """Add a new task to the scheduler"""
        if not self.is_running:
            logger.warning(f"Scheduler not running, cannot add task {task_schedule.id}")
            return
        
        # Remove existing task if it exists
        await self.remove_task(task_schedule.id)
        
        # Calculate next execution time
        next_execution = self._calculate_next_execution(task_schedule)
        if not next_execution:
            logger.info(f"Task {task_schedule.id} has no future executions, skipping")
            return
        
        # Create scheduled task
        scheduled_task = ScheduledTask(
            task_id=task_schedule.id,
            execute_at=next_execution,
            task_schedule=task_schedule,
            is_recurring=task_schedule.interval_seconds is not None
        )
        
        # Add to queue and tracking
        heapq.heappush(self.task_queue, scheduled_task)
        self.active_tasks[task_schedule.id] = scheduled_task
        
        # Update next execution time in database
        await self._update_next_execution_time(task_schedule.id, next_execution)
        
        # Notify scheduler of new task
        self.new_task_event.set()
        
        logger.info(f"Added task {task_schedule.id} ({task_schedule.name}) for execution at {next_execution}")
    
    async def remove_task(self, task_id: int):
        """Remove a task from the scheduler"""
        if task_id in self.active_tasks:
            # Remove from active tasks
            self.active_tasks.pop(task_id)
            
            # Note: We don't remove from heap immediately as it's expensive
            # The scheduler loop will skip tasks that are no longer active
        
            logger.info(f"Removed task {task_id} from scheduler")
        
        # Clear next execution time in database
        await self._update_next_execution_time(task_id, None)
    
    async def update_task(self, task_schedule: TaskSchedule):
        """Update an existing task's schedule"""
        await self.remove_task(task_schedule.id)
        if task_schedule.is_active:
            await self.add_task(task_schedule)
    
    async def get_scheduler_status(self) -> Dict[str, Any]:
        """Get current scheduler status"""
        return {
            "is_running": self.is_running,
            "active_tasks": len(self.active_tasks),
            "running_tasks": len(self.running_tasks),
            "queued_tasks": len(self.task_queue),
            "next_execution": self._get_next_execution_time(),
            "max_concurrent_tasks": self.max_concurrent_tasks
        }
    
    async def _scheduler_loop(self):
        """Main scheduler loop - the heart of the scheduler"""
        logger.info("Scheduler loop started")
        
        try:
            while self.is_running:
                try:
                    self._cleanup_heap()

                    if not self.task_queue:
                        await self._wait_for_tasks_or_shutdown()
                        continue

                    next_task = self.task_queue[0]
                    current_time = datetime.now(pytz.utc)

                    # Check if task is still active
                    if next_task.task_id not in self.active_tasks:
                        heapq.heappop(self.task_queue)
                        continue
                    
                    # Check if it's time to execute
                    if next_task.execute_at <= current_time:
                        task = heapq.heappop(self.task_queue)
                        
                        if len(self.running_tasks) >= self.max_concurrent_tasks:
                            heapq.heappush(self.task_queue, task)
                            await asyncio.sleep(self.scheduler_tick_interval)
                            continue
                        
                        asyncio.create_task(self._execute_and_reschedule(task))
                    else:
                        sleep_time = min(
                            (next_task.execute_at - current_time).total_seconds(),
                            self.scheduler_tick_interval
                        )
                        
                        if sleep_time > 0:
                            await self._wait_with_events(sleep_time)
                
                except Exception as e:
                    logger.error(f"Error in scheduler loop: {e}", exc_info=True)
                    await asyncio.sleep(1)  # Prevent tight error loop
        
        except asyncio.CancelledError:
            logger.info("Scheduler loop cancelled")
        except Exception as e:
            logger.error(f"Scheduler loop failed: {e}", exc_info=True)
        finally:
            logger.info("Scheduler loop ended")
    
    async def _execute_and_reschedule(self, scheduled_task: ScheduledTask):
        """Execute a task and handle rescheduling"""
        task_id = scheduled_task.task_id
        
        try:
            # Mark as running
            self.running_tasks.add(task_id)
            
            logger.info(f"Executing task {task_id} ({scheduled_task.task_schedule.name})")
            
            # Execute the task
            execution = await self.executor.execute_task(scheduled_task.task_schedule)
            
            logger.info(f"Task {task_id} completed with status: {execution.status.value}")
            
            # Handle rescheduling for recurring tasks
            if scheduled_task.is_recurring and task_id in self.active_tasks:
                await self._reschedule_recurring_task(scheduled_task)

            scheduler_status = await self.get_scheduler_status()
            logger.info(f"Scheduler status after task {task_id} execution: {scheduler_status}")

        except Exception as e:
            logger.error(f"Error executing task {task_id}: {e}", exc_info=True)
            
            # For recurring tasks, still try to reschedule
            if scheduled_task.is_recurring and task_id in self.active_tasks:
                await self._reschedule_recurring_task(scheduled_task)
        
        finally:
            # Mark as no longer running
            self.running_tasks.discard(task_id)
    
    async def _reschedule_recurring_task(self, scheduled_task: ScheduledTask):
        """Reschedule a recurring task for its next execution"""
        task_schedule = scheduled_task.task_schedule
        
        # Calculate next execution time
        next_execution = self._calculate_next_execution(task_schedule)
        if not next_execution:
            logger.info(f"Recurring task {task_schedule.id} has no future executions")
            return
        
        # Create new scheduled task
        new_scheduled_task = ScheduledTask(
            task_id=task_schedule.id,
            execute_at=next_execution,
            task_schedule=task_schedule,
            is_recurring=True
        )

        heapq.heappush(self.task_queue, new_scheduled_task)
        self.active_tasks[task_schedule.id] = new_scheduled_task
        
        await self._update_next_execution_time(task_schedule.id, next_execution)
        
        self.new_task_event.set()
        
        logger.info(f"Rescheduled recurring task {task_schedule.id} for {next_execution}")
    
    def _calculate_next_execution(self, task_schedule: TaskSchedule) -> Optional[datetime]:
        """Calculate the next execution time for a task"""
        current_time = datetime.now(pytz.utc)
        
        if task_schedule.scheduled_at:
            scheduled_at = task_schedule.scheduled_at
            if scheduled_at.tzinfo is None:
                scheduled_at = scheduled_at.replace(tzinfo=pytz.utc)
            
            if scheduled_at > current_time:
                logger.info(f"One-time task {task_schedule.id} scheduled for {scheduled_at}")
                return scheduled_at
            else:
                logger.info(f"One-time task {task_schedule.id} past due ({scheduled_at}), skipping")
                return None  # Past due, don't execute
        
        elif task_schedule.interval_seconds:
            if task_schedule.last_executed_at:
                next_time = task_schedule.last_executed_at + timedelta(seconds=task_schedule.interval_seconds)
                logger.info(f"Recurring task {task_schedule.id}: last_executed={task_schedule.last_executed_at}, interval={task_schedule.interval_seconds}s, next={next_time}")
                return next_time
            else:
                logger.info(f"Recurring task {task_schedule.id} never executed, scheduling immediately")
                return current_time
        
        logger.warning(f"Task {task_schedule.id} has no scheduling configuration")
        return None
    
    async def _load_active_tasks(self):
        """Load active tasks from database on startup"""
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(TaskSchedule).where(TaskSchedule.is_active)
            )
            active_task_schedules = result.scalars().all()
            
            for task_schedule in active_task_schedules:
                next_execution = self._calculate_next_execution(task_schedule)
                if next_execution:
                    scheduled_task = ScheduledTask(
                        task_id=task_schedule.id,
                        execute_at=next_execution,
                        task_schedule=task_schedule,
                        is_recurring=task_schedule.interval_seconds is not None
                    )
                    
                    heapq.heappush(self.task_queue, scheduled_task)
                    self.active_tasks[task_schedule.id] = scheduled_task
                    
                    await self._update_next_execution_time(task_schedule.id, next_execution)
        
        logger.info(f"Loaded {len(self.active_tasks)} active tasks from database")
    
    async def _update_next_execution_time(self, task_id: int, next_execution: Optional[datetime]):
        """Update the next_execution_at field in the database"""
        try:
            async with AsyncSessionLocal() as db:
                result = await db.execute(
                    select(TaskSchedule).where(TaskSchedule.id == task_id)
                )
                task_schedule = result.scalar_one_or_none()
                
                if task_schedule:
                    task_schedule.next_execution_at = next_execution
                    await db.commit()
                    logger.debug(f"Updated next_execution_at for task {task_id} to {next_execution}")
                else:
                    logger.warning(f"Task {task_id} not found when updating next_execution_at")
        except Exception as e:
            logger.error(f"Failed to update next_execution_at for task {task_id}: {e}", exc_info=True)
    
    def _cleanup_heap(self):
        """Remove inactive tasks from the heap"""
        active_heap_tasks = [
            task for task in self.task_queue 
            if task.task_id in self.active_tasks
        ]
        
        if len(active_heap_tasks) < len(self.task_queue):
            self.task_queue = active_heap_tasks
            heapq.heapify(self.task_queue)
    
    def _get_next_execution_time(self) -> Optional[str]:
        """Get the next execution time as ISO string"""
        if self.task_queue:
            return self.task_queue[0].execute_at.isoformat()
        return None
    
    async def _wait_for_tasks_or_shutdown(self):
        """Wait for new tasks or shutdown signal"""
        try:
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(self.new_task_event.wait()),
                    asyncio.create_task(self.shutdown_event.wait())
                ],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in pending:
                task.cancel()
            
            if not self.shutdown_event.is_set():
                self.new_task_event.clear()
                
        except asyncio.CancelledError:
            raise
    
    async def _wait_with_events(self, timeout: float):
        """Wait for timeout or events (new task, shutdown)"""
        try:
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(self.new_task_event.wait()),
                    asyncio.create_task(self.shutdown_event.wait()),
                    asyncio.create_task(asyncio.sleep(timeout))
                ],
                return_when=asyncio.FIRST_COMPLETED
            )
            
            for task in pending:
                task.cancel()
            
            # Clear the new task event if it was set
            if not self.shutdown_event.is_set():
                self.new_task_event.clear()
                
        except asyncio.CancelledError:
            raise
