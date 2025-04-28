from pydantic import BaseModel
from typing import Dict, List

class SafetyScoreResponse(BaseModel):
    total_points: int
    earned_points: int
    score_percent: float
    breakdown: Dict[str, int]

class WeeklyScore(BaseModel):
    week: int
    score: float
