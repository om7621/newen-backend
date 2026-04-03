from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pyodbc
import pandas as pd
import io
from urllib.parse import unquote# Added for handling spaces in URLs

app = Flask(__name__)
CORS(app)  # Critical for Flutter Web App support

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
    # Ordered by ID DESC so newest panels appear at the top
    cursor.execute("SELECT panel_serial, project_name, product_type FROM Panels ORDER BY id DESC")
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return jsonify(results)

# 2. GET SECTION DATA
@app.route('/get_section_data', methods=['GET'])
def get_section_data():
    panel = unquote(request.args.get('panel', ''))
    section = unquote(request.args.get('section', ''))
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT component_name, make, serial_number FROM Components WHERE panel_serial = ? AND section_name = ?", panel, section)
    data_map = {}
    for row in cursor.fetchall():
        data_map[row[0]] = {"make": row[1], "serial_number": row[2]}
    conn.close()
    return jsonify(data_map)

# 3. FULL PANEL SYNC (UPSERT)
@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json
    panel = data.get('panel', {})
    components = data.get('components', [])    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    start_date = panel.get('start_date')
    if not start_date or start_date == "":
        start_date = None

    try:
        # --- UPSERT PANEL ---
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
        panel.get('panel_serial'), panel.get('project_name'), panel.get('product_type'), panel.get('prepared_by'), start_date, 
        panel.get('reference_document'), panel.get('verified_by'), panel.get('remarks'), panel.get('status', 'IN_PROGRESS'), panel.get('panel_serial'),
        panel.get('panel_serial'), panel.get('project_name'), panel.get('product_type'), panel.get('prepared_by'), start_date, 
        panel.get('reference_document'), panel.get('verified_by'), panel.get('remarks'), panel.get('status', 'IN_PROGRESS'))

        # --- UPSERT COMPONENTS ---
        for comp in components:
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM Components WHERE panel_serial = ? AND component_name = ?)
                BEGIN
                    UPDATE Components SET section_name = ?, make = ?, serial_number = ?
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
            panel.get('panel_serial'), comp.get('section_name'), comp.get('component_name'), comp.get('make'), comp.get('serial_number'))

        conn.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        print(f"ERROR: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

# 4. EXPORT SINGLE PANEL EXCEL
@app.route('/export_excel', methods=['GET'])
def export_excel():
    panel_serial = unquote(request.args.get('panel', ''))
    conn = get_db_connection()
    try:
        query = """
            SELECT section_name as Section, component_name as Component, 
                   make as Make, serial_number as Serial 
            FROM Components WHERE panel_serial = ?
        """
        df = pd.read_sql(query, conn, params=[panel_serial])
        
        if df.empty:
            return f"No data found for panel: {panel_serial}", 404

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Panel Report')
        output.seek(0)

        return send_file(output, as_attachment=True, download_name=f"Report_{panel_serial}.xlsx")
    except Exception as e:
        return str(e), 500
    finally:
        conn.close()

# 5. EXPORT MASTER EXCEL
@app.route('/export_full_summary', methods=['GET'])
def export_full_summary():
    product_type = request.args.get('product_type', 'CPS3000')
    conn = get_db_connection()
    try:
        panels_df = pd.read_sql("SELECT * FROM Panels WHERE product_type = ?", conn, params=[product_type])
        components_df = pd.read_sql("SELECT panel_serial, component_name, serial_number FROM Components", conn)
        
        if panels_df.empty:
            return f"No {product_type} data found", 404

        if not components_df.empty:
            components_df = components_df.drop_duplicates(subset=['panel_serial', 'component_name'], keep='last')
            pivot_df = components_df.pivot(index='panel_serial', columns='component_name', values='serial_number')
            final_df = panels_df.merge(pivot_df, on='panel_serial', how='left')
        else:
            final_df = panels_df

        column_mapping = {
            'panel_serial': 'Panel Sr. No.',
            'start_date': 'Start Date',
            'project_name': 'Project Name',
            'end_date': 'End Date',
            'product_type': 'Product Type',
            'reference_document': 'W.O/S. O No',
            'prepared_by': 'Prepared By',
            'verified_by': 'Verified By',
            'remarks': 'Remarks'
        }

        final_df.fillna('', inplace=True)
        
        # Only map keys that exist to prevent 'not in index' error
        existing_mapping = {k: v for k, v in column_mapping.items() if k in final_df.columns}
        final_df.rename(columns=existing_mapping, inplace=True)

        meta_headers = list(existing_mapping.values())
        comp_cols = [c for c in final_df.columns if c not in meta_headers and c not in ['id', 'status', 'approved_by']]
        comp_cols.sort()
        
        final_df = final_df[meta_headers + comp_cols]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name=f'{product_type} Master List')
        output.seek(0)

        return send_file(output, as_attachment=True, download_name=f"Master_{product_type}_Report.xlsx")

    except Exception as e:
        return str(e), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
