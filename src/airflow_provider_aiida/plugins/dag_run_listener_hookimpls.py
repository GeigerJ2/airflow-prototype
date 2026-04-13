from airflow.listeners import hookimpl
from airflow.models import DagRun, TaskInstance
from airflow.sdk.execution_time.comms import RuntimeTaskInstance
from airflow.utils.state import TaskInstanceState
from typing import TYPE_CHECKING

# ==========================
# DAGRUN STATE CHANGE EVENTS
# ==========================

@hookimpl
def on_dag_run_running(dag_run: DagRun, msg: str):
    """Called when a DAG run state changes to RUNNING."""
    pass

@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    """Called when a DAG run state changes to SUCCESS."""
    pass

@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    """Called when a DAG run state changes to FAILED."""
    pass

# =================================
# TASK INSTANCE STATE CHANGE EVENTS
# =================================

@hookimpl
def on_task_instance_running(
    previous_state: TaskInstanceState,
    task_instance: RuntimeTaskInstance
):
    """Called when a task instance state changes to RUNNING."""

    print(f"Task {task_instance.task_id} is now running")
    print(f"Previous state: {previous_state}")
    
    # Access task context for more info
    context = task_instance.get_template_context()
    task = context["task"]
    # NOTE: DAG always available
    dag = task.dag


@hookimpl
def on_task_instance_success(
    previous_state: TaskInstanceState,
    task_instance: RuntimeTaskInstance | TaskInstance
):
    pass

@hookimpl
def on_task_instance_failed(
    previous_state: TaskInstanceState,
    task_instance: RuntimeTaskInstance | TaskInstance,
    error: None | str | BaseException,
):
    pass    


# ==============================================================================
# ASSET EVENTS (Data-aware scheduling)
# ==============================================================================

@hookimpl
def on_asset_created(asset):
    """Called when a new asset is created."""
    print(f"Asset created: {asset}")


@hookimpl
def on_asset_alias_created(asset_alias):
    """Called when a new asset alias is created."""
    print(f"Asset alias created: {asset_alias}")


@hookimpl
def on_asset_changed(asset):
    """Called when an asset is changed/updated."""
    print(f"Asset changed: {asset}")


# ==============================================================================
# DAG IMPORT ERROR EVENTS (Experimental)
# ==============================================================================

@hookimpl
def on_new_dag_import_error(filename, stacktrace):
    """
    Called when a new DAG import error is detected.
    
    EXPERIMENTAL FEATURE
    """
    print(f"New import error in {filename}")
    print(f"Stacktrace: {stacktrace}")


@hookimpl
def on_existing_dag_import_error(filename, stacktrace):
    """
    Called when an existing DAG continues to have import errors.
    
    EXPERIMENTAL FEATURE
    """
    print(f"Existing import error in {filename}")
    print(f"Stacktrace: {stacktrace}")


# ==============================================================================
# EXAMPLE: Complete Listener Class
# ==============================================================================

class MyCompleteListener:
    """
    Example listener class implementing multiple hooks.
    
    You don't need to implement all hooks - only the ones you need.
    """
    
    @hookimpl
    def on_dag_run_success(self, dag_run: DagRun, msg: str):
        print(f"[MyListener] DAG {dag_run.dag_id} succeeded!")
    
    @hookimpl
    def on_task_instance_failed(
        self, 
        previous_state: TaskInstanceState,
        task_instance: RuntimeTaskInstance | TaskInstance,
        error: None | str | BaseException
    ):
        print(f"[MyListener] Task failed with error: {error}")


# ==============================================================================
# PLUGIN REGISTRATION
# ==============================================================================

from airflow.plugins_manager import AirflowPlugin

# Create listener instance
my_listener = MyCompleteListener()


class MyListenerPlugin(AirflowPlugin):
    """
    Plugin registration for Airflow.
    
    This is what makes Airflow aware of your listener.
    """
    name = "my_listener_plugin"
    listeners = [my_listener]


# ==============================================================================
# NOTES
# ==============================================================================
"""
Key Points:
-----------
1. DagRun events: running, success, failed
2. TaskInstance events: running, success, failed (with error parameter)
3. Lifecycle events: on_starting, before_stopping
4. Asset events: created, alias_created, changed
5. Import error events: new_dag_import_error, existing_dag_import_error

Task Instance Types:
-------------------
- RuntimeTaskInstance: Normal task execution
- TaskInstance: When state changed via API/UI (Airflow 3+)

Version Compatibility:
---------------------
- error parameter in on_task_instance_failed: Added in 2.10.0
- API-triggered events: Added in 3.0.0 (provides TaskInstance instead of RuntimeTaskInstance)
- Asset events: Added in 3.0.0

You don't need to implement all hooks - only implement what you need!
"""

