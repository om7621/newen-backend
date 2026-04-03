from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pyodbc
import pandas as pd
import io
from urllib.parse import unquote

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
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Sorted by panel_serial DESC since 'id' doesn't exist in your Panels table
        cursor.execute("SELECT panel_serial, project_name, product_type FROM Panels ORDER BY panel_serial DESC")
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        conn.close()
        return jsonify(results)
    except Exception as e:
        print(f"Error: {e}")
        return jsonify([]), 500

# 2. GET SECTION DATA
@app.route('/get_section_data', methods=['GET'])
def get_section_data():
    panel = unquote(request.args.get('panel', ''))
    section = unquote(request.args.get('section', ''))
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT component_name, make, serial_number FROM Components WHERE panel_serial = ? AND section_name = ?", panel, section)
        data_map = {}
        for row in cursor.fetchall():
            data_map[row[0]] = {"make": row[1], "serial_number": row[2]}
        conn.close()
        return jsonify(data_map)
    except Exception as e:
        return jsonify({}), 500

# 3. FULL PANEL SYNC (UPSERT - Matches your schema)
@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json
    panel = data.get('panel', {})
    components = data.get('components', [])    
    
    start_date = panel.get('start_date') if panel.get('start_date') else None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # --- UPSERT PANEL (Removed end_date as per your schema) ---
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

        # --- UPSERT COMPONENTS (Using sync_time as per your schema) ---
        for comp in components:
            cursor.execute("""
                IF EXISTS (SELECT 1 FROM Components WHERE panel_serial = ? AND component_name = ?)
                BEGIN
                    UPDATE Components SET section_name = ?, make = ?, serial_number = ?, sync_time = GETDATE()
                    WHERE panel_serial = ? AND component_name = ?
                END
                ELSE
                BEGIN
                    INSERT INTO Components (panel_serial, section_name, component_name, make, serial_number, sync_time)
                    VALUES (?, ?, ?, ?, ?, GETDATE())
                END
            """, 
            panel.get('panel_serial'), comp.get('component_name'),
            comp.get('section_name'), comp.get('make'), comp.get('serial_number'),
            panel.get('panel_serial'), comp.get('component_name'),
            panel.get('panel_serial'), comp.get('section_name'), comp.get('component_name'), 
            comp.get('make'), comp.get('serial_number'))

        conn.commit()
        conn.close()
        return jsonify({"status": "success"})
    except Exception as e:
        print(f"Sync Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

# 4. EXPORT SINGLE PANEL EXCEL
@app.route('/export_excel', methods=['GET'])
def export_excel():
    panel_serial = unquote(request.args.get('panel', ''))
    try:
        conn = get_db_connection()
        panel_df = pd.read_sql("SELECT * FROM Panels WHERE panel_serial = ?", conn, params=[panel_serial])
        # Using sync_time from your schema
        comp_df = pd.read_sql("SELECT section_name, component_name, make, serial_number, sync_time FROM Components WHERE panel_serial = ?", conn, params=[panel_serial])
        conn.close()

        if panel_df.empty: return "Not Found", 404

        panel_data = panel_df.iloc[0]
        output = io.BytesIO()
        
        # Arrangement order
        order = ["Enclosure", "Fan Box", "Magnetics", "Switchgears", "Sensors", "Resistors", "PCB", "Filter", "Capacitor", "Stack-1", "Stack-2", "Stack-3", "Stack-4", "Power Supply", "U1 STACK", "V1 STACK", "W1 STACK", "U2 STACK", "V2 STACK", "W2 STACK"]
        comp_df['section_order'] = comp_df['section_name'].apply(lambda x: order.index(x) if x in order else 99)
        comp_df = comp_df.sort_values(by=['section_order', 'component_name'])

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            sheet = writer.book.add_worksheet('Traceability')
            bold = writer.book.add_format({'bold': True})
            
            sheet.write(0, 0, "TRACEABILITY REPORT", bold)
            sheet.write(1, 0, f"Panel ID : {panel_serial}")
            sheet.write(2, 0, "Start Date"); sheet.write(2, 1, str(panel_data.get('start_date', '')))
            sheet.write(4, 0, "Project Name"); sheet.write(4, 1, panel_data.get('project_name', ''))
            sheet.write(5, 0, "Panel Sr No"); sheet.write(5, 1, panel_serial)
            sheet.write(6, 0, "Prepared By"); sheet.write(6, 1, panel_data.get('prepared_by', ''))
            sheet.write(7, 0, "Verified By"); sheet.write(7, 1, panel_data.get('verified_by', ''))
            sheet.write(9, 0, "Remarks"); sheet.write(9, 1, panel_data.get('remarks', ''))

            headers = ["Section", "Component", "Make", "Serial", "Time"]
            for col, h in enumerate(headers): sheet.write(11, col, h, bold)

            for i, row in enumerate(comp_df.values):
                for j in range(4): sheet.write(12 + i, j, row[j])
                sheet.write(12 + i, 4, str(row[4]).split('.')[0]) # Format sync_time

            sheet.set_column(0, 4, 25)

        output.seek(0)
        return send_file(output, as_attachment=True, download_name=f"traceability_{panel_serial}.xlsx")
    except Exception as e:
        return str(e), 500

# ========================================================
# 5. EXPORT CPS3000 MASTER EXCEL
# ========================================================
@app.route('/export_cps_summary', methods=['GET'])
def export_cps_summary():
    conn = get_db_connection()
    try:
        panels_df = pd.read_sql("SELECT * FROM Panels WHERE product_type = 'CPS3000'", conn)
        components_df = pd.read_sql("SELECT panel_serial, component_name, serial_number FROM Components", conn)
        
        if panels_df.empty: return "No CPS3000 data found", 404

        if not components_df.empty:
            components_df = components_df.drop_duplicates(subset=['panel_serial', 'component_name'], keep='last')
            pivot_df = components_df.pivot(index='panel_serial', columns='component_name', values='serial_number')
            final_df = panels_df.merge(pivot_df, on='panel_serial', how='left')
        else:
            final_df = panels_df

        column_mapping = {
            'panel_serial': 'Panel Sr. No.', 'start_date': 'Start Date', 'project_name': 'Project Name',
            'product_type': 'Product Type', 'reference_document': 'W.O/S. O No',
            'prepared_by': 'Prepared By', 'verified_by': 'Verified By', 'remarks': 'Remarks'
        }

        # STRICT ORDER FOR CPS3000
        cps_order = [
            "Enclosure Serial No. 1", "Enclosure Serial No. 2",
            "Fan1", "NTC8 – Fan1 – 10K", "Fan2", "NTC10 – Fan2 – 10K",
            "L1", "TR1", "TR2", "L2", "TR3",
            "CB01", "CB02", "K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8",
            "SPD3 – AC SPD", "SPD4 – AC SPD AUX", "SPD1 – DC SPD", "SPD2 – DC SPD",
            "FU1", "FU2", "FU3", "FU4", "ETH2 – ETH SWITCH", "CBF", "CBF1", "CBF2",
            "HCTU1", "HCTV1", "HCTW1", "HCTU2", "HCTV2", "HCTW2", "HCTU3", "HCTV3", "HCTW3", "HCTU4", "HCTV4", "HCTW4",
            "HCTD1", "HCTD2", "NTC7 – P1 – 10K", "NTC9 – P2 – 10K", "A8-1 PT Sensing Board", "A8-2 PT Sensing Board",
            "RA18 – 66KΩ 100W", "RA19 – 66KΩ 100W", "RA20 – 66KΩ 100W", "RA1 – 80E 500W", "RA2 – 80E 500W", "RA3 – 33KΩ 100W", "RA4 – 33KΩ 100W", "RA5 – 33KΩ 100W", "RA6 – 33KΩ 100W", "RA15 – 66KΩ 100W", "RA16 – 66KΩ 100W", "RA17 – 66KΩ 100W",
            #PCB
            "A2-1 Interface Card", "A3-1 Controller Card", "A6-1 CB Card 1", "A7-1 Gate Interlock Card", "A7-2 Gate Interlock Card", "A7-3 Gate Interlock Card", "A12 AC Filter Card", "A13-1 DC Filter", "A1 Domain Controller", "A2-2 Interface Card",
            "A3-2 Controller Card", "A3-3 Controller Card", "A5 Power Supply ORing Card", "A5-1 Power Supply ORing Card", "A6-2 CB Card 2", "A7-4 Gate Interlock Card", "A7-5 Gate Interlock Card", "A7-6 Gate Interlock Card", "A10 SIM100", "A11 Data Logger",
            "A13-2 DC Filter",
            #CAPACITORS
            "Cap Bank CF1", "Cap Bank CF2", "Cap Bank CF3", "Cap Bank CF4", "Cap Bank CF5", "Cap Bank CF6",
            # POWER SUPPLY
            "PS1 – 24V", "PS2 – 24V", "PS3 – 24V", "PS4 – 15V", "PS5 – 12V", "PS6 – 24V", "PS7 – 15V", "PS8 – +/-12V", "PS9 – 24V", "PS10 – 15V", "HMI",
            

            


            "SKYPER1-U1", "SKYPER2-U1", "SKYPER3-U1", "SKYPER4-U1",
            "SKYPER1-V1", "SKYPER2-V1", "SKYPER3-V1", "SKYPER4-V1",
            "SKYPER1-W1", "SKYPER2-W1", "SKYPER3-W1", "SKYPER4-W1",
            "SKYPER1-U2", "SKYPER2-U2", "SKYPER3-U2", "SKYPER4-U2",
            "SKYPER1-V2", "SKYPER2-V2", "SKYPER3-V2", "SKYPER4-V2",
            "SKYPER1-W2", "SKYPER2-W2", "SKYPER3-W2", "SKYPER4-W2"
        ]

        final_df.fillna('', inplace=True)
        existing_rename = {k: v for k, v in column_mapping.items() if k in final_df.columns}
        final_df.rename(columns=existing_rename, inplace=True)

        meta_headers = list(existing_rename.values())
        comp_cols = [c for c in cps_order if c in final_df.columns]
        
        final_df = final_df[meta_headers + comp_cols]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='CPS3000 Summary')
        output.seek(0)
        return send_file(output, as_attachment=True, download_name="Master_CPS3000_Report.xlsx")
    except Exception as e:
        return str(e), 500
    finally:
        conn.close()

# ========================================================
# 6. EXPORT DPS MASTER EXCEL
# ========================================================
@app.route('/export_dps_summary', methods=['GET'])
def export_dps_summary():
    conn = get_db_connection()
    try:
        panels_df = pd.read_sql("SELECT * FROM Panels WHERE product_type = 'DPS'", conn)
        components_df = pd.read_sql("SELECT panel_serial, component_name, serial_number FROM Components", conn)
        
        if panels_df.empty: return "No DPS data found", 404

        if not components_df.empty:
            components_df = components_df.drop_duplicates(subset=['panel_serial', 'component_name'], keep='last')
            pivot_df = components_df.pivot(index='panel_serial', columns='component_name', values='serial_number')
            final_df = panels_df.merge(pivot_df, on='panel_serial', how='left')
        else:
            final_df = panels_df

        column_mapping = {
            'panel_serial': 'Panel Sr. No.', 'start_date': 'Start Date', 'project_name': 'Project Name',
            'product_type': 'Product Type', 'reference_document': 'W.O/S. O No',
            'prepared_by': 'Prepared By', 'verified_by': 'Verified By', 'remarks': 'Remarks'
        }

        # STRICT ORDER FOR DPS
        dps_order = [
            "Enclosure Serial No / Rev No",
            "Fan1", "NTC8 – Fan1 – 10K",
            "L1 (480uH/633A) - 1", "L1 (480uH/633A) - 2", "TV",
            "T1A", "T1B", "T2A", "T2B", "T3A", "T6A", "T6B", "T3", "T4", "T5", "T7", "T8", 
            "FU1 (1500VDC)", "FU2 (1500VDC)", "FU4 (1250A 1500VDC)", "FU8 (1250A 1500VDC)", 
            "ETH2 – ETH SWITCH", "QF1",
            "HALL1", "HALL2", "HALL3", "HALL4", "HALL5",
            "RS1 (HEATER)", "HU1 (HUMIDISTAT)", "KT1", "KT2", "KT3", "KT4", "KT5", "KT6", "KT7", "KT8", "KT9", 
            "R1-R2 100E/150W", "R3-R14 7.5K/60W", "R15-10E/2W", "R16-100E/150W", "R17-100E/150W",
            "SKYPER 1-S1", "SKYPER 2-S2", "SKYPER 3-S3", "SKYPER 4-S4"
        ]

        final_df.fillna('', inplace=True)
        existing_rename = {k: v for k, v in column_mapping.items() if k in final_df.columns}
        final_df.rename(columns=existing_rename, inplace=True)

        meta_headers = list(existing_rename.values())
        comp_cols = [c for c in dps_order if c in final_df.columns]
        
        final_df = final_df[meta_headers + comp_cols]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='DPS Summary')
        output.seek(0)
        return send_file(output, as_attachment=True, download_name="Master_DPS_Report.xlsx")
    except Exception as e:
        return str(e), 500
    finally:
        conn.close()
