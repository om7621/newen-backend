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
@app.route('/export_full_summary', methods=['GET'])
def export_full_summary():
    conn = get_db_connection()
    try:
        # Load all panels
        panels_df = pd.read_sql("SELECT * FROM Panels", conn)
        # Load all components
        components_df = pd.read_sql("SELECT panel_serial, component_name, serial_number FROM Components", conn)
        
        if panels_df.empty:
            return "No data found", 404

        # Pivot components: Makes each component name a column and fills with serial number
        pivot_df = components_df.pivot(index='panel_serial', columns='component_name', values='serial_number')
        
        # Merge panels with their components
        final_df = panels_df.merge(pivot_df, on='panel_serial', how='left')

        # Rename headers to match your specific requirement
        column_mapping = {
            'panel_serial': 'Panel Sr. No.',
            'start_date': 'Start Date',
            'end_date': 'End Date',
            'project_name': 'Project Name',
            'product_type': 'Product Type',
            'reference_document': 'W.O/S. O No',
            'prepared_by': 'Prepared By',
            'verified_by': 'Verified By',
            'remarks': 'Remarks'
        }
        final_df.rename(columns=column_mapping, inplace=True)

        # Arrange: Metadata first, then components
        metadata_cols = list(column_mapping.values())
        component_cols = [c for c in final_df.columns if c not in metadata_cols and c != 'status' and c != 'id']
        final_df = final_df[metadata_cols + component_cols]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='Master Summary')
        output.seek(0)

        return send_file(output, as_attachment=True, download_name="Master_Traceability_Report.xlsx", 
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
        return str(e), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
