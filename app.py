from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pyodbc
import pandas as pd
import io
from urllib.parse import unquote

app = Flask(__name__)
CORS(app)

# Azure SQL Connection
from urllib.parse import unquote # Add this to the top
# ...
connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=newen-server.database.windows.net,1433;" # Verified from your screenshot
    "DATABASE=newen_traceability_db;"
    "UID=omsingh;"
    "PWD=Singhisblink7621;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=120;"
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

        # --- UPSERT COMPONENTS ---
        for comp in components:
            cursor.execute("""IF EXISTS (SELECT 1 FROM Components WHERE panel_serial = ? AND component_name = ?)
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
# ========================================================
# 5. EXPORT CPS3000 MASTER EXCEL (Corrected Order)
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

        # THE COMPLETE CPS3000 LIST (Exactly as you provided)
        cps_order = [
            "Enclosure Serial No. 1", "Enclosure Serial No. 2",
            "Fan1", "NTC8 – Fan1 – 10K", "Fan2", "NTC10 – Fan2 – 10K",
            "L1", "TR1", "TR2", "L2", "TR3", "CB01", "CB02", "K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8",
            "SPD3 – AC SPD", "SPD4 – AC SPD AUX", "SPD1 – DC SPD", "SPD2 – DC SPD",
            "FU1", "FU2", "FU3", "FU4", "ETH2 – ETH SWITCH", "CBF", "CBF1", "CBF2",
            "HCTU1", "HCTV1", "HCTW1", "HCTU2", "HCTV2", "HCTW2", "HCTU3", "HCTV3", "HCTW3", "HCTU4", "HCTV4", "HCTW4",
            "HCTD1", "HCTD2", "NTC7 – P1 – 10K", "NTC9 – P2 – 10K", "A8-1 PT Sensing Board", "A8-2 PT Sensing Board",
            "RA18 – 66KΩ 100W", "RA19 – 66KΩ 100W", "RA20 – 66KΩ 100W", "RA1 – 80E 500W", "RA2 – 80E 500W", "RA3 – 33KΩ 100W", "RA4 – 33KΩ 100W", "RA5 – 33KΩ 100W", "RA6 – 33KΩ 100W", "RA15 – 66KΩ 100W", "RA16 – 66KΩ 100W", "RA17 – 66KΩ 100W",
            "A2-1 Interface Card", "A3-1 Controller Card", "A6-1 CB Card 1", "A7-1 Gate Interlock Card", "A7-2 Gate Interlock Card", "A7-3 Gate Interlock Card", "A12 AC Filter Card", "A13-1 DC Filter", "A1 Domain Controller", "A2-2 Interface Card",
            "A3-2 Controller Card", "A3-3 Controller Card", "A5 Power Supply ORing Card", "A5-1 Power Supply ORing Card", "A6-2 CB Card 2", "A7-4 Gate Interlock Card", "A7-5 Gate Interlock Card", "A7-6 Gate Interlock Card", "A10 SIM100", "A11 Data Logger", "A13-2 DC Filter",
            "Cap Bank CF1", "Cap Bank CF2", "Cap Bank CF3", "Cap Bank CF4", "Cap Bank CF5", "Cap Bank CF6",
            "PS1 – 24V", "PS2 – 24V", "PS3 – 24V", "PS4 – 15V", "PS5 – 12V", "PS6 – 24V", "PS7 – 15V", "PS8 – +/-12V", "PS9 – 24V", "PS10 – 15V", "HMI",
            # Stack U1
            "A4-1", "A4-2", "IGBT1", "IGBT2", "IGBT3", "IGBT4", "TS1 – 120°C", "TS2 – 120°C", "NTC1 – 10K", "CD1-8", "SKYPER1-U1", "SKYPER2-U1", "SKYPER3-U1", "SKYPER4-U1",
            # Stack V1
            "A4-3", "A4-4", "IGBT5", "IGBT6", "IGBT7", "IGBT8", "TS3 – 120°C", "TS4 – 120°C", "NTC2 – 10K", "CD9-16", "SKYPER1-V1", "SKYPER2-V1", "SKYPER3-V1", "SKYPER4-V1",
            # Stack W1
            "A4-5", "A4-6", "IGBT9", "IGBT10", "IGBT11", "IGBT12", "TS5 – 120°C", "TS6 – 120°C", "NTC3 – 10K", "CD17-24", "SKYPER1-W1", "SKYPER2-W1", "SKYPER3-W1", "SKYPER4-W1",
            # Stack U2
            "A4-7", "A4-8", "IGBT13", "IGBT14", "IGBT15", "IGBT16", "TS7 – 120°C", "TS8 – 120°C", "CD25-32", "NTC4 – 10K", "SKYPER1-U2", "SKYPER2-U2", "SKYPER3-U2", "SKYPER4-U2",
            # Stack V2
            "A4-9", "A4-10", "IGBT17", "IGBT18", "IGBT19", "IGBT20", "TS9 – 120°C", "TS10 – 120°C", "CD33-40", "NTC5 – 10K", "SKYPER1-V2", "SKYPER2-V2", "SKYPER3-V2", "SKYPER4-V2",
            # Stack W2
            "A4-11", "A4-12", "IGBT21", "IGBT22", "IGBT23", "IGBT24", "TS11 – 120°C", "TS12 – 120°C", "CD41-48", "NTC6 – 10K", "SKYPER1-W2", "SKYPER2-W2", "SKYPER3-W2", "SKYPER4-W2"
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
# 6. EXPORT DPS MASTER EXCEL (Corrected Order)
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

        # THE COMPLETE DPS LIST (Exactly as you provided)
        dps_order = [
            "Enclosure Serial No / Rev No",
            "Fan1", "NTC8 – Fan1 – 10K",
            "L1 (480uH/633A) - 1", "L1 (480uH/633A) - 2", "TV",
            "T1A", "T1B", "T2A", "T2B", "T3A", "T6A", "T6B", "T3", "T4", "T5", "T7", "T8", 
            "FU1 (1500VDC)", "FU2 (1500VDC)", "FU4 (1250A 1500VDC)", "FU5 (1250A 1500VDC)", 
            "ETH2 – ETH SWITCH", "QF1",
            "HALL1", "HALL2", "HALL3", "HALL4", "HALL5",
            "RS1 (HEATER)", "HU1 (HUMIDISTAT)", "KT1", "KT2", "KT3", "KT4", "KT5", "KT6", "KT7", "KT8", "KT9", 
            "R1-R2 100E/150W", "R3-R14 7.5K/60W", "R15-10E/2W", "R16-100E/150W", "R17-100E/150W",
            "Controller (NI Board)", "DPS A1 - Interface Board", "DPS A2 - Power Supply Board - 24VDC", "DPS A3 - Power Supply Board - 15VDC", "DPS A3-1 Power Supply Board - 15VDC", "DPS IGBT Driver Board A4",
            "DPS IGBT Driver Board A5", "DPS IGBT Driver Board A6", "DPS IGBT Driver Board A7", "DPS A12 Fan Controller Board",
            "DPS A13 Signal Detection and Power Transfer Board", "DPS A14 Contactor Power Board",
            "DPS Filter Board FL1", "DPS Filter Board FL2", "DPS Filter Board FL3", "DPS Filter Board FL4", "FILTER-5",
            "C1-C12", "C13-C24",
            # Stacks
            "DPS IGBT Adaptor Board A8A-S1", "DPS IGBT Adaptor Board A8B-S1", "DPS IGBT Adaptor Board A8C-S1", "(IGBT) Q1-A", "(IGBT) Q1-B", "(IGBT) Q1-C", "SKYPER 1",
            "DPS IGBT Adaptor Board A9A-S2", "DPS IGBT Adaptor Board A9B-S2", "DPS IGBT Adaptor Board A9C-S2", "(IGBT) Q2-A", "(IGBT) Q2-B", "(IGBT) Q2-C", "SKYPER 2",
            "DPS IGBT Adaptor Board A10A-S3", "DPS IGBT Adaptor Board A10B-S3", "DPS IGBT Adaptor Board A10C-S3", "(IGBT) Q3-A", "(IGBT) Q3-B", "(IGBT) Q3-C", "SKYPER 3",
            "DPS IGBT Adaptor Board A11A-S4", "DPS IGBT Adaptor Board A11B-S4", "DPS IGBT Adaptor Board A11C-S4", "(IGBT) Q4-A", "(IGBT) Q4-B", "(IGBT) Q4-C", "SKYPER 4"
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
        
# ========================================================
# 7. EXPORT DPS 2500 MASTER EXCEL (Complete Ordered List)
# ========================================================
@app.route('/export_dps2500_summary', methods=['GET'])
def export_dps2500_summary():
    conn = get_db_connection()
    try:
        panels_df = pd.read_sql("SELECT * FROM Panels WHERE product_type = 'DPS 2500'", conn)
        components_df = pd.read_sql("SELECT panel_serial, component_name, serial_number FROM Components", conn)
        
        if panels_df.empty: return "No DPS 2500 data found", 404

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

        # THE COMPLETE DPS 2500 LIST (Strictly following your provided order)
        dps2500_order = [
            "Fan1", "NTC8 – Fan1 – 10K", "Fan2", "NTC10 – Fan2 – 10K",
            "L1", "L2", "TR1", "TR2", "TR3", "TR4",
            "CB01", "CB02", "K1", "K2", "K3", "K4", "K5", "K6", "K7", "K8",
            "FU1", "FU2", "FU3", "FU4", "FU5 – FU7", "FC1 – FU6", "ETH2 – ETH SWITCH", "CBF", "SW1", "SW2",
            "HCTU1", "HCTV1", "HCTW1", "HCTU2", "HCTV2", "HCTW2", "HCTU3", "HCTV3", "HCTW3", "HCTU4", "HCTV4", "HCTW4", "HCTD1", "HCTD2", "NTC7 – P1 – 10K", "NTC9 – P2 – 10K",
            "RA1 – 80E 500W", "RA2 – 80E 500W", "RA3 – RA8 – 100kΩ 50W", "RA9 – RA14 – 100kΩ 50W", "RA15 – RA17 – 60kΩ 50W", "RA18 – RA20 – 60kΩ 50W",
            "A1: Domain controller", "A2-1: Interface card", "A2-2: Interface card", "A3-1: Controller card", "A3-2: Controller card", "A3-3: Controller card", "A5: Power Supply Oring card", "A5-1: Power Supply Oring card", "A6-1: ACB card", "A6-2: ACB card", "A7-1 to A7-6: Gate Interlock cards", "A10: SIM 100",
            "Cap Bank – CF1 – CF9", "Cap Bank – CF10 – CF18",
            "PS1 – 24V", "PS2 – 24V", "PS3 – 24V", "PS4 – 15V", "PS5 – 12V", "PS6 – 24V", "PS7 – 15V", "PS8 – ±12V", "PS9 – 24V", "PS10 – 15V",
            "HMI",
            "TR5", "KP1 – KP4", "PS11", "DIODE D1 – D4", "RA21 – RA22 – 80E 500W", "HU1, HU2", "RS1, RS2", "FU8 – FU26", "DS1, DS2 (Door Switch)", "IS01 (ISOMETER & COUPLING)",
            # U1 Stack
            "A4-1", "A4-2", "IGBT1", "IGBT2", "IGBT3", "IGBT4", "TS1 – 120°C", "TS2 – 120°C", "CD1 – CD6", "NTC1 – 10K", "SKYPER 1 U1", "SKYPER 2 U1", "SKYPER 3 U1", "SKYPER 4 U1",
            # V1 Stack
            "A4-3", "A4-4", "IGBT5", "IGBT6", "IGBT7", "IGBT8", "TS3 – 120°C", "TS4 – 120°C", "CD7 – CD14", "NTC2 – 10K", "SKYPER 1 V1", "SKYPER 2 V1", "SKYPER 3 V1", "SKYPER 4 V1",
            # W1 Stack
            "A4-5", "A4-6", "IGBT9", "IGBT10", "IGBT11", "IGBT12", "TS5 – 120°C", "TS6 – 120°C", "CD15 – CD21", "NTC3 – 10K", "SKYPER 1 W1", "SKYPER 2 W1", "SKYPER 3 W1", "SKYPER 4 W1",
            # U2 Stack
            "A4-7", "A4-8", "IGBT13", "IGBT14", "IGBT15", "IGBT16", "TS7 – 120°C", "TS8 – 120°C", "CD22 – CD28", "NTC4 – 10K", "SKYPER 1 U2", "SKYPER 2 U2", "SKYPER 3 U2", "SKYPER 4 U2",
            # V2 Stack
            "A4-9", "A4-10", "IGBT17", "IGBT18", "IGBT19", "IGBT20", "TS9 – 120°C", "TS10 – 120°C", "CD29 – CD35", "NTC5 – 10K", "SKYPER 1 V2", "SKYPER 2 V2", "SKYPER 3 V2", "SKYPER 4 V2",
            # W2 Stack
            "A4-11", "A4-12", "IGBT21", "IGBT22", "IGBT23", "IGBT24", "TS11 – 120°C", "TS12 – 120°C", "CD36 – CD42", "NTC6 – 10K", "SKYPER 1 W2", "SKYPER 2 W2", "SKYPER 3 W2", "SKYPER 4 W2"
        ]

        final_df.fillna('', inplace=True)
        existing_rename = {k: v for k, v in column_mapping.items() if k in final_df.columns}
        final_df.rename(columns=existing_rename, inplace=True)

        meta_headers = list(existing_rename.values())
        comp_cols = [c for c in dps2500_order if c in final_df.columns]
        
        final_df = final_df[meta_headers + comp_cols]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            final_df.to_excel(writer, index=False, sheet_name='DPS 2500 Summary')
        output.seek(0)
        return send_file(output, as_attachment=True, download_name="Master_DPS2500_Report.xlsx")
    except Exception as e:
        return str(e), 500
    finally:
        conn.close()
