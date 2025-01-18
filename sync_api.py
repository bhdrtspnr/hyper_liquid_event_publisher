from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
from scraper import scrape_positions
from typing import List, Optional, Dict, Any

app = FastAPI(title="HyperLiquid Position Tracker")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store previous positions
previous_positions: Optional[pd.DataFrame] = None

class PositionChange(BaseModel):
    timestamp: str
    vault_address: str
    data: Dict[str, Any]

class PositionResponse(BaseModel):
    changes: List[PositionChange]
    current_positions: List[Dict[str, Any]]

def detect_position_changes(current_positions: pd.DataFrame) -> tuple[list, list]:
    global previous_positions
    
    if previous_positions is None:
        previous_positions = current_positions
        return [], []  # First call, no changes to report
    
    # Get sets of assets from previous and current positions
    prev_assets = set(previous_positions['asset'].values)
    curr_assets = set(current_positions['asset'].values)
    
    # Find closed positions (in previous but not in current)
    closed_positions = []
    for asset in prev_assets - curr_assets:
        position = previous_positions[previous_positions['asset'] == asset].to_dict('records')[0]
        position['event_type'] = 'POSITION_CLOSED'
        closed_positions.append(position)
    
    # Find new positions (in current but not in previous)
    opened_positions = []
    for asset in curr_assets - prev_assets:
        position = current_positions[current_positions['asset'] == asset].to_dict('records')[0]
        position['event_type'] = 'POSITION_OPENED'
        opened_positions.append(position)
    
    previous_positions = current_positions
    return closed_positions, opened_positions

@app.get("/positions", response_model=PositionResponse)
async def get_positions():
    """
    Get current positions and changes since last call.
    Returns both the current positions and any changes detected since the last API call.
    """
    vault_url = "https://app.hyperliquid.xyz/vaults/0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7"
    
    try:
        # Fetch current positions
        current_positions = scrape_positions(vault_url)
        
        # Detect changes
        closed_positions, opened_positions = detect_position_changes(current_positions)
        
        # Format changes
        changes = []
        all_changes = closed_positions + opened_positions
        
        for change in all_changes:
            changes.append(PositionChange(
                timestamp=datetime.utcnow().isoformat(),
                vault_address="0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7",
                data=change
            ))
        
        # Convert current positions to list of dicts
        current_positions_list = current_positions.to_dict('records')
        
        return PositionResponse(
            changes=changes,
            current_positions=current_positions_list
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 