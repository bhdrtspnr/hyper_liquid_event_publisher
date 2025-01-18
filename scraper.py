from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import atexit

# Global driver instance
driver = None

def setup_driver():
    global driver
    if driver is None:
        chrome_options = Options()
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--start-maximized')
        driver = webdriver.Chrome(options=chrome_options)
        # Register cleanup function
        atexit.register(cleanup_driver)
    return driver

def cleanup_driver():
    global driver
    if driver:
        try:
            driver.quit()
        except:
            pass
        driver = None

def scrape_positions(url):
    global driver
    try:
        driver = setup_driver()
        # Refresh the page instead of creating new window
        driver.get(url)
        
        # Wait for positions to load
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "sc-oZIhv")))
        
        # Give time for all data to load
        time.sleep(5)
        
        positions = []
        rows = driver.find_elements(By.CLASS_NAME, "sc-oZIhv")
        
        for row in rows:
            cells = row.find_elements(By.CLASS_NAME, "sc-hiDMwi")
            if cells:
                # Extract asset and leverage from first cell
                asset_cell = cells[0].text.split()
                asset = asset_cell[0]
                leverage = asset_cell[1].strip('x')
                
                # Extract position size and direction
                size_text = cells[1].text.strip()
                direction = "LONG" if "LONG" in size_text else "SHORT"
                size = size_text.replace("LONG", "").replace("SHORT", "").strip()
                
                position = {
                    'asset': asset,
                    'leverage': leverage,
                    'direction': direction,
                    'size': size,
                    'notional_value': cells[2].text,
                    'entry_price': cells[3].text,
                    'mark_price': cells[4].text,
                    'pnl': cells[5].text,
                    'liq_price': cells[6].text,
                    'margin': cells[7].text,
                    'funding': cells[8].text
                }
                positions.append(position)
        
        return pd.DataFrame(positions)
    
    except Exception as e:
        print(f"Error during scraping: {str(e)}")
        # If there's an error, cleanup and allow for new driver creation
        cleanup_driver()
        raise

if __name__ == "__main__":
    vault_url = "https://app.hyperliquid.xyz/vaults/0x8fc7c0442e582bca195978c5a4fdec2e7c5bb0f7"
    df = scrape_positions(vault_url)
    
    # Display the results
    print("\nVault Positions:")
    print(df.to_string())
    
    # Save to CSV
    df.to_csv('vault_positions.csv', index=False)
    print("\nPositions saved to vault_positions.csv")
    
    # Cleanup when running as main
    cleanup_driver()
