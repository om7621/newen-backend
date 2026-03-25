from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pyodbc
import pandas as pd
import io

app = Flask(__name__)
CORS(app) # Ensure this is present to allow Web App access

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

@app.route('/get_section_data', methods=['GET'])
def get_section_data():
    panel = request.args.get('panel')
    section = request.args.get('section')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    # Explicitly using indices [0, 1, 2] is safer for Web-to-Python data mapping
    cursor.execute("SELECT component_name, make, serial_number FROM Components WHERE panel_serial = ? AND section_name = ?", panel, section)
    
    data_map = {}
    for row in cursor.fetchall():
        data_map[row[0]] = {"make": row[1], "serial": row[2]}
    
    conn.close()
    return jsonify(data_map)

@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json
    panel = data['panel']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Improved UPSERT: Updates the panel if it already exists, otherwise inserts
        # This fixes the "Start Assembly" button if a panel was already started on another device
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM Panels WHERE panel_serial = ?)
            BEGIN
                UPDATE Panels SET project_name = ?, product_type = ?, prepared_by = ?, start_date = ?, reference_document = ?, verified_by = ?, remarks = ?, status = ?
                WHERE panel_serial = ?
            END
            ELSE
            BEGIN
                INSERT INTO Panels (panel_serial, project_name, product_type, prepared_by, start_date, reference_document, verified_by, remarks, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            END
        """, panel['panel_serial'], panel['project_name'], panel['product_type'], panel['prepared_by'], panel['start_date'] or None, panel['reference_document'], panel['verified_by'], panel['remarks'], panel['status'], panel['panel_serial'],
             panel['panel_serial'], panel['project_name'], panel['product_type'], panel['prepared_by'], panel['start_date'] or None, panel['reference_document'], panel['verified_by'], panel['remarks'], panel['status'])
        
        conn.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

@app.route('/export_excel', methods=['GET'])
def export_excel():
    panel_serial = request.args.get('panel')
    conn = get_db_connection()
    query = "SELECT section_name, component_name, make, serial_number FROM Components WHERE panel_serial = ?"
    df = pd.read_sql(query, conn, params=[panel_serial])
    conn.close()
    
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Traceability')
    output.seek(0)
    
    return send_file(output, as_attachment=True, download_name=f"Report_{panel_serial}.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
