from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pyodbc
import pandas as pd
import io

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
    cursor.execute("SELECT panel_serial, project_name, product_type FROM Panels ORDER BY panel_serial DESC")
    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]
    conn.close()
    return jsonify(results)

# 2. GET SECTION DATA (Shared between devices)
@app.route('/get_section_data', methods=['GET'])
def get_section_data():
    panel = request.args.get('panel')
    section = request.args.get('section')
    conn = get_db_connection()
    cursor = conn.cursor()
    # Pulls exactly what is needed for the Component Entry Screen
    cursor.execute("SELECT component_name, make, serial_number FROM Components WHERE panel_serial = ? AND section_name = ?", panel, section)
    data_map = {}
    for row in cursor.fetchall():
        data_map[row[0]] = {"make": row[1], "serial_number": row[2]}
    conn.close()
    return jsonify(data_map)

# 3. FULL PANEL SYNC (With UPSERT - Prevents Data Loss)
@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json
    panel = data.get('panel', {})
    components = data.get('components', [])    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Handle potentially empty dates for new panels
    start_date = panel.get('start_date')
    if not start_date or start_date == "":
        start_date = None

    try:
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

        # Component UPSERT logic remains the same...
        conn.commit()
        return jsonify({"status": "success"})
    except Exception as e:
        print(f"ERROR: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    finally:
        conn.close()

# 4. EXPORT MASTER EXCEL (Organized by your custom workflow)
@app.route('/export_full_summary', methods=['GET'])
def export_full_summary():
    conn = get_db_connection()
    try:
        panels_df = pd.read_sql("SELECT * FROM Panels", conn)
        components_df = pd.read_sql("SELECT panel_serial, component_name, serial_number FROM Components", conn)
        
        if panels_df.empty:
            return "No panel data found", 404

        # 1. Pivot components (Unique names like SKYPER1-U1 ensure no overwriting)
        if not components_df.empty:
            components_df = components_df.drop_duplicates(subset=['panel_serial', 'component_name'], keep='last')
            pivot_df = components_df.pivot(index='panel_serial', columns='component_name', values='serial_number')
            final_df = panels_df.merge(pivot_df, on='panel_serial', how='left')
        else:
            final_df = panels_df

        # 2. Define Headers and Sorting order
        metadata_mapping = {
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

        # Components in your preferred order
        ordered_components = [
            "Enclosure Serial No. 1", "Enclosure Serial No. 2",
            "Fan1", "NTC8 – Fan1 – 10K", "Fan2", "NTC10 – Fan2 – 10K",
            "L1", "TR1", "TR2", "L2", "TR3",
            "CB01", "CB02", "K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8",
            "SPD3 – AC SPD", "SPD4 – AC SPD AUX", "SPD1 – DC SPD", "SPD2 – DC SPD",
            "FU1", "FU2", "FU3", "FU4", "ETH2 – ETH SWITCH", "CBF", "CBF1", "CBF2",
            "HCTU1", "HCTV1", "HCTW1", "HCTU2", "HCTV2", "HCTW2", "HCTU3", "HCTV3", "HCTW3", "HCTU4", "HCTV4", "HCTW4",
            "HCTD1", "HCTD2", "NTC7 – P1 – 10K", "NTC9 – P2 – 10K", "A8-1 PT Sensing Board", "A8-2 PT Sensing Board"
        ]

        # Add Stack components dynamically
        for s in ["U1", "V1", "W1", "U2", "V2", "W2"]:
            # Logic to generate names like SKYPER1-U1, IGBT1-U1, etc.
            stack_comps = [f"A4-{x}-{s}" for x in range(1, 13)] + \
                          [f"IGBT{x}-{s}" for x in range(1, 25)] + \
                          [f"SKYPER{x}-{s}" for x in range(1, 5)]
            ordered_components.extend(stack_comps)

        # 3. Clean and Arrange DataFrame
        final_df.fillna('', inplace=True) # Blank spaces for missing data
        final_df.rename(columns=metadata_mapping, inplace=True)

        meta_headers = list(metadata_mapping.values())
        existing_ordered_cols = [c for c in meta_headers + ordered_components if c in final_df.columns]
        
        # Capture any columns not in our list (safety)
        remaining_cols = [c for c in final_df.columns if c not in existing_ordered_cols and c not in ['id', 'status', 'approved_by']]
        
        final_df = final_df[existing_ordered_cols + remaining_cols]

        # 4. Generate the File
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='Master Summary')
        output.seek(0)

        return send_file(output, as_attachment=True, download_name="Master_Traceability_Report.xlsx")

    except Exception as e:
        return str(e), 500
    finally:
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
