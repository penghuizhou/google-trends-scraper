from pytrends.request import TrendReq
import pandas as pd
from datetime import datetime
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment
import os
import time
import sys

def log_message(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def pull_google_trends():
    """Main function to pull Google Trends data and save to Excel"""
    
    try:
        log_message("Starting Google Trends data pull...")
        
        # Initialize pytrends with retry logic
        log_message("Initializing connection to Google Trends...")
        pytrends = TrendReq(hl='en-US', tz=360)

        
        # Define queries
        queries = ['Marble countertop', 'home remodel']
        log_message(f"Queries: {', '.join(queries)}")
        
        # Build payload for all-time data (monthly)
        log_message("Requesting data from Google Trends (timeframe: all-time, geo: US)...")
        pytrends.build_payload(queries, cat=0, timeframe='all', geo='US', gprop='')
        
        # Add small delay to avoid rate limiting
        time.sleep(2)
        
        # Get interest over time data
        log_message("Fetching interest over time data...")
        df = pytrends.interest_over_time()
        
        if df.empty:
            log_message("ERROR: No data returned from Google Trends")
            return False
        
        log_message(f"Retrieved {len(df)} data points")
        
        # Remove 'isPartial' column if it exists
        if 'isPartial' in df.columns:
            df = df.drop('isPartial', axis=1)
        
        # Reset index to make date a column
        df = df.reset_index()
        
        # Rename columns for clarity
        df = df.rename(columns={'date': 'Month'})
        
        # Output file path - saves in same directory as script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_file = os.path.join(script_dir, 'google_trends_data.xlsx')
        
        # Check if file exists
        file_exists = os.path.exists(output_file)
        
        if file_exists:
            log_message("Existing file found. Updating with new data...")
            # Append new data to existing file
            existing_df = pd.read_excel(output_file, sheet_name='Trends Data')
            
            # Add pull date column to new data
            df['Pull Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Append new data
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            
            # Remove duplicates based on Month, keeping the most recent pull
            original_count = len(combined_df)
            combined_df = combined_df.sort_values('Pull Date', ascending=False)
            combined_df = combined_df.drop_duplicates(subset=['Month'], keep='first')
            combined_df = combined_df.sort_values('Month')
            
            duplicates_removed = original_count - len(combined_df)
            if duplicates_removed > 0:
                log_message(f"Removed {duplicates_removed} duplicate entries")
            
            # Write to Excel
            combined_df.to_excel(output_file, sheet_name='Trends Data', index=False)
        else:
            log_message("Creating new file...")
            # Create new file
            df['Pull Date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df.to_excel(output_file, sheet_name='Trends Data', index=False)
        
        # Format the Excel file
        log_message("Formatting Excel file...")
        wb = load_workbook(output_file)
        ws = wb['Trends Data']
        
        # Format headers
        header_font = Font(bold=True)
        for cell in ws[1]:
            cell.font = header_font
            cell.alignment = Alignment(horizontal='center')
        
        # Auto-adjust column widths
        for column in ws.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = min(max_length + 2, 50)
            ws.column_dimensions[column_letter].width = adjusted_width
        
        wb.save(output_file)
        
        # Success message
        log_message("SUCCESS!")
        log_message(f"File saved to: {output_file}")
        log_message(f"Date range: {df['Month'].min()} to {df['Month'].max()}")
        log_message(f"Total records in file: {len(combined_df) if file_exists else len(df)}")
        
        return True
        
    except Exception as e:
        log_message(f"ERROR: {str(e)}")
        log_message(f"Error type: {type(e).__name__}")
        return False

if __name__ == "__main__":
    success = pull_google_trends()
    sys.exit(0 if success else 1)
