# ls_py_handler/main.py
from fastapi import FastAPI
from ls_py_handler.config.settings import settings

# IMPORTANT: import init_app from the module where your optimized runs.py lives
# If your file is ls_py_handler/api/runs.py:
from ls_py_handler.api.routes.runs import init_app

app = FastAPI(
    title=settings.APP_TITLE,
    description=settings.APP_DESCRIPTION,
    version=settings.APP_VERSION,
)

# Wire up startup/shutdown (DB pool, S3 client, semaphore) and include /runs router
init_app(app)

@app.get("/")
async def root():
    """Simple health/info endpoint."""
    return {"message": f"{settings.APP_TITLE} API"}
