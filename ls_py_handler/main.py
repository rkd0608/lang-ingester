from fastapi import FastAPI
from ls_py_handler.api.routes.runs import router as runs_router

app = FastAPI(
    title="LS Run Handler",
    description="A simple FastAPI server with run endpoints",
    version="0.1.0"
)

# Include routers
app.include_router(runs_router)


@app.get("/")
async def root():
    """
    Root endpoint to verify the API is running.
    """
    return {"message": "LS Run Handler API"}
