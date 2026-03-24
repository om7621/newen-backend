from flask import Flask, request, jsonify
import pyodbc
import json

app = Flask(__name__)

# Azure SQL Connection
# Ensure ODBC Driver 18 is installed on your machine
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

# 1. GET ALL PANELS (For Cloud List)
@app.route('/get_panels', methods=['GET'])
def get_panels():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT panel_serial, project_name, product_type FROM Panels ORDER BY panel_serial DESC")
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return jsonify(results)

# 2. FULL SYNC (Panel + All Components)
@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json
    panel = data['panel']
    components = data['components']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Upsert Panel
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Panels WHERE panel_serial = ?)
            INSERT INTO Panels (panel_serial, project_name, product_type, prepared_by, start_date, reference_document, verified_by, remarks, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, panel['panel_serial'], panel['panel_serial'], panel['project_name'], panel['product_type'], 
             panel['prepared_by'], panel['start_date'], panel['reference_document'], panel['verified_by'], panel['remarks'], panel['status'])

        # Sync Components
        for comp in components:
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Components WHERE panel_serial = ? AND section_name = ? AND component_name = ?)
                INSERT INTO Components (panel_serial, section_name, component_name, make, serial_number)
                VALUES (?, ?, ?, ?, ?)
            """, comp['panel'], comp['section'], comp['component'], 
                 comp['panel'], comp['section'], comp['component'], comp['make'], comp['serial'])
        
        conn.commit()
        return jsonify({"status": "success", "message": "Full panel synced"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

# 3. SECTION SYNC (From inside a section)
@app.route('/sync_section', methods=['POST'])
def sync_section():
    data = request.json
    panel_serial = data['panel_serial']
    section_name = data['section_name']
    items = data['data']
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Clear old data for this section to prevent duplicates
        cursor.execute("DELETE FROM Components WHERE panel_serial = ? AND section_name = ?", panel_serial, section_name)
        
        for item in items:
            cursor.execute("""
                INSERT INTO Components (panel_serial, section_name, component_name, make, serial_number)
                VALUES (?, ?, ?, ?, ?)
            """, panel_serial, section_name, item['component'], item['make'], item['serial'])
        
        conn.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

# 4. GET SECTION DATA (For Cloud Fetch)
@app.route('/get_section_data', methods=['GET'])
def get_section_data():
    panel = request.args.get('panel')
    section = request.args.get('section')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT component_name, make, serial_number FROM Components WHERE panel_serial = ? AND section_name = ?", panel, section)
    
    data_map = {}
    for row in cursor.fetchall():
        data_map[row.component_name] = {"make": row.make, "serial": row.serial_number}
    
    conn.close()
    return jsonify(data_map)

if __name__ == '__main__':
    # Use 0.0.0.0 to make it accessible on your local network
    app.run(host='0.0.0.0', port=5000, debug=True)