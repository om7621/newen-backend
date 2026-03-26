from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pyodbc
import pandas as pd
import io

app = Flask(__name__)
CORS(app)  # Allow web app access

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


# =========================
# 1. GET ALL PANELS
# =========================
@app.route('/get_panels', methods=['GET'])
def get_panels():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT panel_serial, project_name, product_type 
        FROM Panels 
        ORDER BY panel_serial DESC
    """)

    columns = [column[0] for column in cursor.description]
    results = [dict(zip(columns, row)) for row in cursor.fetchall()]

    conn.close()
    return jsonify(results)


# =========================
# 2. GET SECTION DATA
# =========================
@app.route('/get_section_data', methods=['GET'])
def get_section_data():
    panel = request.args.get('panel')
    section = request.args.get('section')

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT component_name, make, serial_number 
        FROM Components 
        WHERE panel_serial = ? AND section_name = ?
    """, panel, section)

    data_map = {}
    for row in cursor.fetchall():
        data_map[row[0]] = {
            "make": row[1],
            "serial_number": row[2]  # ✅ FIXED KEY
        }

    conn.close()
    return jsonify(data_map)


# =========================
# 3. FULL PANEL SYNC
# =========================
@app.route('/sync_full_panel', methods=['POST'])
def sync_full_panel():
    data = request.json

    print("FULL REQUEST:", data)
    
    panel = data.get('panel', {})
    components = data.get('components', [])

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # ================= PANEL UPSERT =================
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
                INSERT INTO Panels (
                    panel_serial, project_name, product_type, prepared_by,
                    start_date, reference_document, verified_by, remarks, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            END
        """,
        panel.get('panel_serial'),
        panel.get('project_name'),
        panel.get('product_type'),
        panel.get('prepared_by'),
        panel.get('start_date') or None,
        panel.get('reference_document'),
        panel.get('verified_by'),
        panel.get('remarks'),
        panel.get('status'),
        panel.get('panel_serial'),

        panel.get('panel_serial'),
        panel.get('project_name'),
        panel.get('product_type'),
        panel.get('prepared_by'),
        panel.get('start_date') or None,
        panel.get('reference_document'),
        panel.get('verified_by'),
        panel.get('remarks'),
        panel.get('status')
        )

        # ================= COMPONENTS SYNC =================
        # Clear old components
        cursor.execute(
            "DELETE FROM Components WHERE panel_serial = ?",
            panel.get('panel_serial')
        )

        # Insert new components
        for comp in components:
            cursor.execute("""
                INSERT INTO Components (
                    panel_serial, section_name, component_name, make, serial_number
                )
                VALUES (?, ?, ?, ?, ?)
            """,
            panel.get('panel_serial'),
            comp.get('section_name'),
            comp.get('component_name'),
            comp.get('make'),
            comp.get('serial_number')
            )

        conn.commit()
        return jsonify({"status": "success"})

    except Exception as e:
        print("ERROR:", e)
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        conn.close()

# =========================
# 4. EXPORT EXCEL
# =========================
@app.route('/export_excel', methods=['GET'])
def export_excel():
    panel_serial = request.args.get('panel')

    conn = get_db_connection()

    query = """
        SELECT section_name, component_name, make, serial_number 
        FROM Components 
        WHERE panel_serial = ?
    """

    df = pd.read_sql(query, conn, params=[panel_serial])
    conn.close()

    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Traceability')

    output.seek(0)

    return send_file(
        output,
        as_attachment=True,
        download_name=f"Report_{panel_serial}.xlsx",
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )


# =========================
# 5. HOME ROUTE
# =========================
@app.route('/')
def home():
    return "Newen Traceability Backend Running 🚀"


# =========================
# RUN APP
# =========================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
