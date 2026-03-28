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

# 4. EXPORT MASTER EXCEL REPORT (One row per panel)
@app.route('/export_full_summary', methods=['GET'])
def export_full_summary():
    conn = get_db_connection()
    try:
        # 1. Fetch Panels
        panels_df = pd.read_sql("SELECT * FROM Panels", conn)
        if panels_df.empty:
            return "No data found", 404

        # 2. Fetch Components
        components_df = pd.read_sql("SELECT panel_serial, section_name, component_name, serial_number FROM Components", conn)
        
        # 3. Process STACK components to make names unique
        def rename_stack_comp(row):
            sec = str(row['section_name']).upper()
            comp = str(row['component_name'])
            if "STACK" in sec:
                stack_id = sec.split(" ")[0] # Gets 'U1', 'V1', etc.
                return f"{comp}-{stack_id}"
            return comp

        if not components_df.empty:
            components_df['unique_name'] = components_df.apply(rename_stack_comp, axis=1)
            components_df = components_df.drop_duplicates(subset=['panel_serial', 'unique_name'], keep='last')
            pivot_df = components_df.pivot(index='panel_serial', columns='unique_name', values='serial_number')
            final_df = panels_df.merge(pivot_df, on='panel_serial', how='left')
        else:
            final_df = panels_df

        # 4. Define EXACT column order
        metadata_cols = {
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

        # Add STACK components dynamically (U1, V1, W1, U2, V2, W2)
        stacks = ["U1", "V1", "W1", "U2", "V2", "W2"]
        for i, s in enumerate(stacks):
            # Matches your CPS3000Template numbering logic
            ordered_components.extend([
                f"A4-{i*2+1}-{s}", f"A4-{i*2+2}-{s}",
                f"IGBT{i*4+1}-{s}", f"IGBT{i*4+2}-{s}", 
                f"IGBT{i*4+3}-{s}", f"IGBT{i*4+4}-{s}",
                f"TS{i*2+1} – 120°C-{s}", f"TS{i*2+2} – 120°C-{s}",
                f"CD{i*8+1}-{i*8+8}-{s}" if i < 5 else f"CD41-48-{s}", # Handles your capacitor naming
                f"NTC{i+1} – 10K-{s}",
                f"SKYPER1-{s}", f"SKYPER2-{s}", f"SKYPER3-{s}", f"SKYPER4-{s}"
            ])

        # Rename and Clean
        final_df.fillna('', inplace=True)
        final_df.rename(columns=metadata_cols, inplace=True)

        # Build final column list based on what actually exists
        header_list = list(metadata_cols.values())
        all_possible_cols = header_list + ordered_components
        
        existing_cols = [c for c in all_possible_cols if c in final_df.columns]
        extra_cols = [c for c in final_df.columns if c not in existing_cols and c not in ['id', 'status', 'approved_by']]
        
        final_df = final_df[existing_cols + extra_cols]

        # 5. Generate Excel
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='Master Summary')
        output.seek(0)

        return send_file(output, as_attachment=True, download_name="Master_Traceability_Report.xlsx")

    except Exception as e:
        print(f"ERROR: {e}")
        return str(e), 500
    finally:
        conn.close()
