"""
Main entry point for the Task Scheduler API
"""
import uvicorn

def main():
    """Run the FastAPI application"""
    uvicorn.run(
        "app.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,  # Disable reload in production/Docker
        log_level="info"
    )

if __name__ == "__main__":
    main()
