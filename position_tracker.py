from typing import List, Dict, Any, Tuple
import pandas as pd

class PositionTracker:
    def __init__(self):
        self.previous_positions = None

    def detect_position_changes(self, current_positions: pd.DataFrame) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Detect opened and closed positions by comparing current positions with previous state.
        
        Args:
            current_positions (pd.DataFrame): DataFrame containing current positions
            
        Returns:
            Tuple[List[Dict], List[Dict]]: Lists of closed and opened positions
        """
        if self.previous_positions is None:
            self.previous_positions = current_positions
            return [], []  # First call, no changes to report
        
        # Get sets of assets from previous and current positions
        prev_assets = set(self.previous_positions['asset'].values)
        curr_assets = set(current_positions['asset'].values)
        
        # Find closed positions (in previous but not in current)
        closed_positions = []
        for asset in prev_assets - curr_assets:
            position = self.previous_positions[self.previous_positions['asset'] == asset].to_dict('records')[0]
            position['event_type'] = 'POSITION_CLOSED'
            closed_positions.append(position)
        
        # Find new positions (in current but not in previous)
        opened_positions = []
        for asset in curr_assets - prev_assets:
            position = current_positions[current_positions['asset'] == asset].to_dict('records')[0]
            position['event_type'] = 'POSITION_OPENED'
            opened_positions.append(position)
        
        self.previous_positions = current_positions
        return closed_positions, opened_positions 