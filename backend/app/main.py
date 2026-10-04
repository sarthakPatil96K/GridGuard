from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.app.routers import (
    dashboard,
    meters,
    anomalies,
    analytics,
    priorities,
    alerts
)


app = FastAPI(
    title="GridGuard API",
    description="Electricity Consumption Anomaly and Theft-Risk Analytics API",
    version="1.0.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(dashboard.router)
app.include_router(meters.router)
app.include_router(anomalies.router)
app.include_router(analytics.router)
app.include_router(priorities.router)
app.include_router(alerts.router)

@app.get("/")
def root():

    return {
        "project": "GridGuard",
        "status": "running",
        "version": "1.0.0"
    }