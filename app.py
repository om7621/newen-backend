from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pyodbc
import pandas as pd
import io

app = Flask(__name__)
CORS(app)

# Azure SQL Connection
connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=newen-server.database.windows.net,1433;"
    "DATABASE=newen_traceability_db;"
    "UID=omsingh;"
    "PWD=Singhisblink7621;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=30;"
)

def get_db_connection():
    return pyodbc.connect(connection_string)

@app.route('/')
def home():
    return "Newen Traceability Backend Running 🚀"

# 1. GET ALL PANELS
@app.route('/get_panels', methods=['GET'])
def get_panels():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT panel_serial, project_name, product_type FROM Panels ORDER BY panel_serial DESC")
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return jsonify(results)

# 2. GET SECTION DATA
@app.route('/get_section_data', methods=['GET'])
def get_section_data():
    panel = request.args.get('panel')
    section = request.args.get('section')
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT component_name, make, serial_number FROM Components WHERE panel_serial = ? AND section_name = ?", panel, section)
    data_map = {}
    for row in cursor.fetchall():
        data_map[row[0]] = {"make": row[1], "serial_number": row[2]}
    conn.close()
    return jsonify(data_map)

# 3. FULL PANEL SYNC (With UPSERT)
@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json
    panel = data.get('panel', {})
    components = data.get('components', [])
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Update or Insert Panel
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM Panels WHERE panel_serial = ?)
            BEGIN
                UPDATE Panels SET project_name=?, product_type=?, prepared_by=?, start_date=?, reference_document=?, verified_by=?, remarks=?, status=?
                WHERE panel_serial = ?
            END
            ELSE
            BEGIN
                INSERT INTO Panels (panel_serial, project_name, product_type, prepared_by, start_date, reference_document, verified_by, remarks, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            END
        """, 
        panel.get('panel_serial'), panel.get('project_name'), panel.get('product_type'), panel.get('prepared_by'), panel.get('start_date'), panel.get('reference_document'), panel.get('verified_by'), panel.get('remarks'), panel.get('status'), panel.get('panel_serial'),
        panel.get('panel_serial'), panel.get('project_name'), panel.get('product_type'), panel.get('prepared_by'), panel.get('start_date'), panel.get('reference_document'), panel.get('verified_by'), panel.get('remarks'), panel.get('status'))

        # Sync Components (Delete old, Insert new to keep it clean)
        cursor.execute("DELETE FROM Components WHERE panel_serial = ?", panel.get('panel_serial'))
        for comp in components:
            cursor.execute("INSERT INTO Components (panel_serial, section_name, component_name, make, serial_number) VALUES (?, ?, ?, ?, ?)",
                         panel.get('panel_serial'), comp.get('section_name'), comp.get('component_name'), comp.get('make'), comp.get('serial_number'))
        
        conn.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

# 4. EXPORT MASTER EXCEL REPORT (One row per panel)
@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json
    panel = data.get('panel', {})
    components = data.get('components', [])

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 1. UPSERT PANEL (This part is already good)
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM Panels WHERE panel_serial = ?)
            BEGIN
                UPDATE Panels 
                SET project_name = ?, product_type = ?, prepared_by = ?, 
                    start_date = ?, reference_document = ?, verified_by = ?, 
                    remarks = ?, status = ?
                WHERE panel_serial = ?
            END
            ELSE
            BEGIN
                INSERT INTO Panels (panel_serial, project_name, product_type, prepared_by,
                                  start_date, reference_document, verified_by, remarks, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            END
        """,
        panel.get('panel_serial'), panel.get('project_name'), panel.get('product_type'),
        panel.get('prepared_by'), panel.get('start_date'), panel.get('reference_document'),
        panel.get('verified_by'), panel.get('remarks'), panel.get('status'), panel.get('panel_serial'),
        panel.get('panel_serial'), panel.get('project_name'), panel.get('product_type'),
        panel.get('prepared_by'), panel.get('start_date'), panel.get('reference_document'),
        panel.get('verified_by'), panel.get('remarks'), panel.get('status'))

        # 2. SMART COMPONENT SYNC (UPSERT instead of DELETE ALL)
        for comp in components:
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM Components WHERE panel_serial = ? AND component_name = ?)
                BEGIN
                    UPDATE Components 
                    SET section_name = ?, make = ?, serial_number = ?
                    WHERE panel_serial = ? AND component_name = ?
                END
                ELSE
                BEGIN
                    INSERT INTO Components (panel_serial, section_name, component_name, make, serial_number)
                    VALUES (?, ?, ?, ?, ?)
                END
            """,
            panel.get('panel_serial'), comp.get('component_name'),
            comp.get('section_name'), comp.get('make'), comp.get('serial_number'),
            panel.get('panel_serial'), comp.get('component_name'),
            panel.get('panel_serial'), comp.get('section_name'), comp.get('component_name'), 
            comp.get('make'), comp.get('serial_number'))

        conn.commit()
        return jsonify({"status": "success"})

    except Exception as e:
        print("ERROR:", e)
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()
