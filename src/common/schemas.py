from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional

class Task(BaseModel):
    task_id: str
    operation: str
    parameters: Dict[str, Any]
    dependencies: List[str] = []

class WorkflowRequest(BaseModel):
    workflow_id: str
    dataset_id: str  # Add this field to identify the dataset
    inputs: Optional[Dict[str, str]] = None  # Make inputs optional
    tasks: List[Task]