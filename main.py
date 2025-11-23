# app.py
import os
import time
import requests
import json
import psycopg2
import sqlite3
from urllib.parse import urlparse
from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

app = Flask(__name__)

# --- CONFIGURATION ---
LOTTERY_API_URL = "https://draw.ar-lottery01.com/WinGo/WinGo_1M/GetHistoryIssuePage.json"
# Render provides DATABASE_URL if you add a Postgres DB. 
# If not found, it falls back to local sqlite (only good for local testing).
DATABASE_URL = os.environ.get('DATABASE_URL')

def get_db_connection():
    """Connects to Postgres (Render) or SQLite (Local)."""
    if DATABASE_URL:
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        conn = sqlite3.connect('server_history.db')
        conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize the table to store results."""
    conn = get_db_connection()
    cur = conn.cursor()
    # Standard SQL compatible with both PG and SQLite
    cur.execute("""
        CREATE TABLE IF NOT EXISTS history (
            issue TEXT PRIMARY KEY,
            code INTEGER,
            open_time TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()
    print("Database initialized.")

# --- THE WORKER: Fetches data from external API ---
def fetch_job():
    print(f"[{datetime.now()}] Fetching data...")
    try:
        payload = {'pageSize': 20, 'pageIndex': 1}
        r = requests.get(LOTTERY_API_URL, params=payload, timeout=10)
        r.raise_for_status()
        data = r.json().get('data', {}).get('list', [])
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        new_count = 0
        for item in data:
            issue = str(item['issueNumber'])
            code = int(item['number'])
            open_time = item['openTime']
            
            # Insert if not exists (ON CONFLICT DO NOTHING is PG syntax, INSERT OR IGNORE is SQLite)
            # We use a generic approach using SELECT first to be safe across both
            cur.execute("SELECT issue FROM history WHERE issue = %s" if DATABASE_URL else "SELECT issue FROM history WHERE issue = ?", (issue,))
            if not cur.fetchone():
                if DATABASE_URL:
                    cur.execute("INSERT INTO history (issue, code, open_time) VALUES (%s, %s, %s)", (issue, code, open_time))
                else:
                    cur.execute("INSERT INTO history (issue, code, open_time) VALUES (?, ?, ?)", (issue, code, open_time))
                new_count += 1
        
        conn.commit()
        conn.close()
        if new_count > 0:
            print(f"[{datetime.now()}] Saved {new_count} new records.")
            
    except Exception as e:
        print(f"Error in fetch job: {e}")

# --- THE API: Your local bot will call this ---
@app.route('/')
def home():
    return "Lottery Server is Running. Use /api/history to get data."

@app.route('/api/history')
def get_history():
    """Returns the last 50 results in JSON format."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get last 50 sorted by issue descending
        if DATABASE_URL:
            cur.execute("SELECT issue, code, open_time FROM history ORDER BY issue DESC LIMIT 50")
        else:
            cur.execute("SELECT issue, code, open_time FROM history ORDER BY issue DESC LIMIT 50")
            
        rows = cur.fetchall()
        conn.close()
        
        # Format exactly like the original API so your bot doesn't break
        formatted_list = []
        for row in rows:
            # Handle row access depending on DB type
            if DATABASE_URL:
                # Psycopg2 returns tuples
                formatted_list.append({
                    'issueNumber': row[0],
                    'number': str(row[1]),
                    'openTime': row[2]
                })
            else:
                # SQLite Row object
                formatted_list.append({
                    'issueNumber': row['issue'],
                    'number': str(row['code']),
                    'openTime': row['open_time']
                })
                
        return jsonify({
            "code": 0,
            "msg": "Success",
            "data": {
                "totalCount": len(formatted_list),
                "list": formatted_list
            }
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- STARTUP ---
if __name__ == '__main__':
    # Run DB init once
    init_db()
    
    # Start Background Scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=fetch_job, trigger="interval", seconds=20)
    scheduler.start()
    
    # Start Flask
    # Render uses PORT env var
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
