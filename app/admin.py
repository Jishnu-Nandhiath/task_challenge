"""
Admin interface using SQLAdmin for automatic admin panel generation
"""
from sqladmin import Admin, ModelView

from app.models import TaskSchedule, TaskExecution, CounterState


class TaskScheduleAdmin(ModelView, model=TaskSchedule):
    """Admin view for TaskSchedule model"""
    column_list = [
        TaskSchedule.id,
        TaskSchedule.name,
        TaskSchedule.task_type,
        TaskSchedule.is_active,
        TaskSchedule.interval_seconds,
        TaskSchedule.scheduled_at,
        TaskSchedule.created_at,
        TaskSchedule.last_executed_at,
        TaskSchedule.next_execution_at,
    ]
    column_details_list = [
        TaskSchedule.id,
        TaskSchedule.name,
        TaskSchedule.description,
        TaskSchedule.task_type,
        TaskSchedule.task_config,
        TaskSchedule.interval_seconds,
        TaskSchedule.scheduled_at,
        TaskSchedule.is_active,
        TaskSchedule.created_at,
        TaskSchedule.updated_at,
        TaskSchedule.last_executed_at,
        TaskSchedule.next_execution_at,
    ]
    column_searchable_list = [TaskSchedule.name, TaskSchedule.description]
    column_sortable_list = [
        TaskSchedule.id,
        TaskSchedule.name,
        TaskSchedule.task_type,
        TaskSchedule.is_active,
        TaskSchedule.created_at,
        TaskSchedule.last_executed_at,
    ]
    

    name = "Task Schedule"
    name_plural = "Task Schedules"
    icon = "fa-solid fa-clock"
    
    page_size = 20
    page_size_options = [10, 20, 50, 100]


class TaskExecutionAdmin(ModelView, model=TaskExecution):
    """Admin view for TaskExecution model"""
    column_list = [
        TaskExecution.id,
        TaskExecution.task_id,
        TaskExecution.status,
        TaskExecution.started_at,
        TaskExecution.completed_at,
        TaskExecution.execution_time_ms,
    ]
    column_details_list = [
        TaskExecution.id,
        TaskExecution.task_id,
        TaskExecution.status,
        TaskExecution.started_at,
        TaskExecution.completed_at,
        TaskExecution.execution_time_ms,
        TaskExecution.result_data,
        TaskExecution.error_message,
    ]
    column_searchable_list = [TaskExecution.error_message]
    column_sortable_list = [
        TaskExecution.id,
        TaskExecution.task_id,
        TaskExecution.status,
        TaskExecution.started_at,
        TaskExecution.completed_at,
        TaskExecution.execution_time_ms,
    ]

    form_columns = []  

    can_delete = True       

    name = "Task Execution"
    name_plural = "Task Executions"
    icon = "fa-solid fa-play"
    
    page_size = 50
    page_size_options = [25, 50, 100, 200]


class CounterStateAdmin(ModelView, model=CounterState):
    """Admin view for CounterState model"""
    column_list = [
        CounterState.id,
        CounterState.task_id,
        CounterState.counter_value,
        CounterState.updated_at,
    ]
    column_details_list = [
        CounterState.id,
        CounterState.task_id,
        CounterState.counter_value,
        CounterState.updated_at,
    ]
    column_sortable_list = [
        CounterState.id,
        CounterState.task_id,
        CounterState.counter_value,
        CounterState.updated_at,
    ]

    name = "Counter State"
    name_plural = "Counter States"
    icon = "fa-solid fa-calculator"
    
    page_size = 20
    page_size_options = [10, 20, 50, 100]


def setup_admin(app, engine):
    """Setup SQLAdmin with the FastAPI app"""
    admin = Admin(app, engine, title="Task Scheduler Admin")
    
    admin.add_view(TaskScheduleAdmin)
    admin.add_view(TaskExecutionAdmin)
    admin.add_view(CounterStateAdmin)
    
    return admin
