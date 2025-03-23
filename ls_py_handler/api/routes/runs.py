from fastapi import APIRouter, status
from uuid import UUID

router = APIRouter(prefix="/runs", tags=["runs"])


@router.post("", status_code=status.HTTP_201_CREATED)
async def create_run():
    """
    Create a new run.
    """
    return {"status": "created"}


@router.get("/{run_id}", status_code=status.HTTP_200_OK)
async def get_run(run_id: UUID):
    """
    Get a run by its ID.
    """
    return {"status": "retrieved", "id": str(run_id)}
