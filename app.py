# -*- coding: utf-8 -*-
"""
Sistema Integral de Vacunación PAI 2026
Incluye: Dashboard Web, Sincronización ETL Segura y Exportación a Excel de Alto Rendimiento.
"""

import os
import io
import json
import logging
import time
import random
import re
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from io import StringIO, BytesIO
from functools import wraps
import datetime

import jwt
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from sqlalchemy import create_engine, text
from werkzeug.security import check_password_hash
from flask import Flask, jsonify, request, send_from_directory, g, send_file, Response
from flask_cors import CORS
from dotenv import load_dotenv

load_dotenv()
DIR_BASE = os.path.dirname(os.path.abspath(__file__))
SECRET_KEY = os.getenv("SECRET_KEY", "vacunacion-aps-ese-2026-secret")

logging.basicConfig(level=logging.INFO, format='%(asctime)s | [%(levelname)s] | %(message)s')
logger = logging.getLogger("VACUNACION_API")

app = Flask(__name__, static_folder=DIR_BASE)
CORS(app)


# =====================================================================
# CONEXIÓN A BASE DE DATOS Y CONFIGURACIÓN DE SEGURIDAD
# =====================================================================
def get_engine():
    db_user = os.getenv("DB_USER_AIVEN", os.getenv("DB_USER"))
    db_password = os.getenv("DB_PASSWORD_AIVEN", os.getenv("DB_PASSWORD"))
    db_host = os.getenv("DB_HOST_AIVEN", os.getenv("DB_HOST"))
    db_port = os.getenv("DB_PORT_AIVEN", os.getenv("DB_PORT", "13505"))
    db_name = os.getenv("DB_NAME_AIVEN", os.getenv("DB_NAME", "defaultdb"))
    cadena = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?sslmode=require"
    return create_engine(cadena, pool_pre_ping=True)


engine = get_engine()


def generar_token(user_id: int, correo: str, nombre: str, rol: str) -> str:
    payload = {
        "user_id": user_id, "correo": correo, "nombre": nombre, "rol": rol,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")


def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": "Token requerido"}), 401
        try:
            payload = jwt.decode(auth.split(" ")[1], SECRET_KEY, algorithms=["HS256"])
            g.user = payload
        except:
            return jsonify({"error": "Token inválido o expirado"}), 401
        return f(*args, **kwargs)

    return decorated


# =====================================================================
# ENRUTAMIENTO DE INTERFACES WEB (HTML)
# =====================================================================
@app.route("/")
@app.route("/login")
def html_login_page():
    return send_from_directory(DIR_BASE, "login.html")


@app.route("/dashboard")
def html_dashboard_page():
    return send_from_directory(DIR_BASE, "dashboard.html")


@app.route("/gestion")
def html_gestion_page():
    return send_from_directory(DIR_BASE, "gestion.html")


@app.route("/logo-ese.png")
def logoese_page(): return send_from_directory(DIR_BASE, "logo-ese.png")


@app.route("/logo-aps.png")
def logoaps_page(): return send_from_directory(DIR_BASE, "logo-aps.png")


# =====================================================================
# ENDPOINTS DE LA API CORE (JSON)
# =====================================================================
@app.route("/api/login", methods=["POST"])
def api_login_auth():
    body = request.get_json(silent=True) or {}
    correo = str(body.get("correo", "")).strip().lower()
    password = str(body.get("password", "")).strip()

    if not correo or not password:
        return jsonify({"error": "Credenciales requeridas"}), 400

    try:
        with engine.connect() as conn:
            res = conn.execute(
                text("SELECT id, username, password_hash FROM usuarios WHERE LOWER(TRIM(username)) = :u LIMIT 1"),
                {"u": correo})
            usuario = res.mappings().first()

            if not usuario or not check_password_hash(usuario["password_hash"], password):
                return jsonify({"error": "Credenciales incorrectas"}), 401

            nombre_visual = usuario["username"].capitalize()
            token = generar_token(usuario["id"], usuario["username"], nombre_visual, "Admin")
            return jsonify({"token": token, "nombre": nombre_visual, "rol": "Admin"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/vacunacion/datos", methods=["GET"])
@require_auth
def api_get_datos_vacunacion():
    try:
        query = 'SELECT * FROM public."vacunacion_aps_2026"'
        df = pd.read_sql(query, engine)

        if df.empty:
            return jsonify({"registros": [], "columnas_db": []})

        col_tipo = next((c for c in df.columns if "tipo_de_vacunaci" in c.lower()), None)
        df['fecha_filtro'] = df['created_at'].astype(str).str.slice(0, 10) if 'created_at' in df.columns else ''
        df['tipo'] = df[col_tipo] if col_tipo else 'Sin Clasificar'
        df['lat'] = df['lat_1_geopunto'] if 'lat_1_geopunto' in df.columns else None
        df['lng'] = df['long_1_geopunto'] if 'long_1_geopunto' in df.columns else None

        columnas_df = list(df.columns)
        df = df.fillna('')
        registros_dict = df.to_dict(orient='records')

        datos_json = json.dumps({
            "registros": registros_dict,
            "columnas_db": columnas_df
        }, ensure_ascii=False, default=str)

        return Response(datos_json, mimetype='application/json')
    except Exception as e:
        logger.error(f"❌ Error en /api/vacunacion/datos: {str(e)}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/vacunacion/sync", methods=["POST"])
@require_auth
def api_sync_vacunacion():
    log_stream = io.StringIO()
    try:
        etl = ETLVacunacion(log_stream)
        etl.procesar()
        return jsonify({"status": "success", "logs": log_stream.getvalue()})
    except Exception as e:
        return jsonify({"status": "error", "logs": log_stream.getvalue() + f"\n❌ ERROR: {str(e)}"})


@app.route("/api/vacunacion/exportar", methods=["POST"])
@require_auth
def api_exportar_vacunacion():
    body = request.get_json(silent=True) or {}
    f_ini = body.get("fecha_ini", "")
    f_fin = body.get("fecha_fin", "")

    log_stream = io.StringIO()
    try:
        motor = ExcelVacunacion(log_stream)
        archivo_io = motor.generar(f_ini, f_fin)

        nombre_archivo = f"Reporte_Vacunacion_{int(time.time())}.xlsx"
        ruta_archivo = os.path.join(DIR_BASE, nombre_archivo)
        with open(ruta_archivo, "wb") as f:
            f.write(archivo_io.getbuffer())

        return jsonify(
            {"status": "success", "logs": log_stream.getvalue(), "download_url": f"/download/{nombre_archivo}"})
    except Exception as e:
        return jsonify({"status": "error", "logs": log_stream.getvalue() + f"\n❌ ERROR: {str(e)}"})


@app.route("/download/<filename>")
def download_file(filename):
    return send_file(os.path.join(DIR_BASE, filename), as_attachment=True)


# =====================================================================
# CLASE LOGICA: PIPELINE ETL (EXTRACCIÓN Y TRADUCCIÓN DE FECHAS)
# =====================================================================
class ETLVacunacion:
    def __init__(self, log_stream):
        self.client_id = os.getenv("VACUNACION_2026_CLIENT_ID")
        self.client_secret = os.getenv("VACUNACION_2026_CLIENT_SECRET")
        self.project_slug = os.getenv("API_PROJECT_SLUG_VACUNACION_2026")
        self.base_url = "https://five.epicollect.net/api"
        self.logger = logging.getLogger("ETL")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
        self.logger.handlers = [handler]

        self.session = self._configurar_sesion()
        self._autenticar_api()

    def _configurar_sesion(self):
        s = requests.Session()
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retries))
        return s

    def _autenticar_api(self):
        self.logger.info("Solicitando Token de Epicollect5...")
        payload = {'grant_type': 'client_credentials', 'client_id': self.client_id, 'client_secret': self.client_secret}
        resp = self.session.post(f"{self.base_url}/oauth/token", data=payload)
        resp.raise_for_status()
        self.session.headers.update({'Authorization': f"Bearer {resp.json()['access_token']}"})
        self.logger.info("Autenticación exitosa.")

    def _ejecutar_upsert_seguro(self, tabla_temporal: str, tabla_final: str, columna_pk: str):
        with engine.begin() as conn:
            res_dest = conn.execute(text(
                f"SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{tabla_final}'"))
            cols_destino = {row[0]: row[1] for row in res_dest.fetchall()}

            res_temp = conn.execute(text(
                f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{tabla_temporal}'"))
            cols_temporal = [row[0] for row in res_temp.fetchall()]

            columnas_comunes = [c for c in cols_destino if c in cols_temporal]

            nombres_insert = []
            nombres_select = []
            updates = []

            for col in columnas_comunes:
                tipo = cols_destino[col].lower()
                nombres_insert.append(f'"{col}"')

                if 'timestamp' in tipo or 'date' in tipo:
                    expr = f"""
                    (CASE 
                        WHEN NULLIF(TRIM("{col}"), '') IS NULL THEN NULL
                        WHEN TRIM("{col}") ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN 
                            CASE WHEN TRIM("{col}") ~ ' ' THEN to_timestamp(TRIM("{col}"), 'YYYY-MM-DD HH24:MI:SS')
                                 ELSE to_date(TRIM("{col}"), 'YYYY-MM-DD')::timestamp END
                        WHEN TRIM("{col}") ~ '^[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}' THEN 
                            CASE WHEN TRIM("{col}") ~ ' ' THEN to_timestamp(TRIM("{col}"), 'DD/MM/YYYY HH24:MI:SS')
                                 ELSE to_date(TRIM("{col}"), 'DD/MM/YYYY')::timestamp END
                        ELSE NULL 
                    END)
                    """
                    nombres_select.append(expr)
                elif 'numeric' in tipo or 'double' in tipo or 'real' in tipo:
                    nombres_select.append(f'CAST(NULLIF("{col}", \'\') AS NUMERIC)')
                elif 'int' in tipo:
                    nombres_select.append(f'CAST(NULLIF("{col}", \'\') AS BIGINT)')
                else:
                    nombres_select.append(f'CAST("{col}" AS TEXT)')

                if col != columna_pk:
                    updates.append(f'"{col}" = EXCLUDED."{col}"')

            sql_upsert = f"""
                INSERT INTO public."{tabla_final}" ({", ".join(nombres_insert)})
                SELECT {", ".join(nombres_select)} FROM public."{tabla_temporal}"
                ON CONFLICT ("{columna_pk}") 
                DO UPDATE SET {", ".join(updates)};
            """
            conn.execute(text(sql_upsert))

    def procesar(self):
        self.logger.info("Iniciando extracción de EpiCollect5 (Vacunación)...")
        form_ref = os.getenv("API_FORM_REF_VACUNACION_2026")
        tabla_destino = "vacunacion_aps_2026"
        columna_pk = "ec5_uuid"

        pagina = 1
        total_procesados = 0
        tabla_temporal = f"temp_{tabla_destino}"
        ids_activos = set()
        parametros = {'form_ref': form_ref, 'format': 'csv', 'per_page': 500}

        while True:
            if pagina > 1:
                time.sleep(random.uniform(2.0, 4.5))

            self.logger.info(f"Descargando bloque de datos (Página {pagina})...")
            parametros['page'] = pagina
            respuesta = None
            error_400_detectado = False

            for intento in range(3):
                try:
                    respuesta = self.session.get(f"{self.base_url}/export/entries/{self.project_slug}",
                                                 params=parametros, timeout=45)

                    if respuesta.status_code == 400:
                        self.logger.error("ERROR 400 Detectado.")
                        error_400_detectado = True
                        break
                    if respuesta.status_code == 429:
                        self.logger.warning("⚠️ Límite de velocidad (429) alcanzado. Pausando proceso 20s...")
                        time.sleep(20)
                        continue
                    if respuesta.status_code == 401:
                        self._autenticar_api()
                        continue

                    respuesta.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    if intento == 2: break
                    time.sleep(5)

            if error_400_detectado or not respuesta or respuesta.status_code != 200: break
            if len(respuesta.text.splitlines()) <= 1: break

            df = pd.read_csv(StringIO(respuesta.text), dtype=str)
            df.columns = [str(c).strip().replace(" ", "_").replace("-", "_").lower() for c in df.columns]
            df = df.replace(['nan', 'NaN', 'None', 'null', 'NULL', ''], None)

            if columna_pk in df.columns: ids_activos.update(df[columna_pk].dropna().tolist())
            total_procesados += len(df)

            with engine.begin() as conn:
                df.to_sql(tabla_temporal, conn, if_exists='replace', index=False)

            self._ejecutar_upsert_seguro(tabla_temporal, tabla_destino, columna_pk)
            pagina += 1

        if ids_activos:
            self.logger.info("Saneando registros eliminados en la nube...")
            with engine.begin() as conn:
                pd.DataFrame({columna_pk: list(ids_activos)}).to_sql("temp_ids_vac", conn, if_exists='replace',
                                                                     index=False)
                res = conn.execute(text(
                    f'DELETE FROM public."{tabla_destino}" WHERE "{columna_pk}" NOT IN (SELECT "{columna_pk}" FROM temp_ids_vac)'))
                self.logger.info(f"Se eliminaron {res.rowcount} registros obsoletos.")
        self.logger.info(f"--- Proceso finalizado. Total activos sincronizados: {total_procesados} ---")


# =====================================================================
# CLASE LOGICA: EXPORTACIÓN VECTORIAL ULTRA-RÁPIDA Y A PRUEBA DE FALLOS
# =====================================================================
class ExcelVacunacion:
    def __init__(self, log_stream):
        self.logger = logging.getLogger("EXCEL")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler(log_stream)
        handler.setFormatter(logging.Formatter('%(message)s'))
        self.logger.handlers = [handler]

        self.mapeo_automatico = {}
        self.mapeo_exacto = {
            "ec5_uuid": "ID Ficha Epicollect", "created_at": "Fecha de Creacion (API)",
            "uploaded_at": "Fecha de Sincronizacion", "title": "Titulo del Registro",
            "created_by": "Usuario Creador", "229_tipo_de_identifi": "TIPO DE IDENTIFICACIÓN",
            "230_numero_de_identi": "NUMERO DE IDENTIFICACIÓN", "231_fecha_de_nacimie": "FECHA DE NACIMIENTO",
            "232_primer_apellido": "PRIMER APELLIDO", "233_segundo_apellido": "SEGUNDO APELLIDO",
            "234_primer_nombre": "PRIMER NOMBRE", "235_segundo_nombre": "SEGUNDO NOMBRE",
            "60_primer_apellido_d": "PRIMER APELLIDO DEL NIÑO", "61_segundo_apellido_": "SEGUNDO APELLIDO DEL NIÑO",
            "62_primer_nombre_del": "PRIMER NOMBRE DEL NIÑO", "63_segundo_nombre_de": "SEGUNDO NOMBRE DEL NIÑO"
        }

    def _limpiar_texto(self, texto: str) -> str:
        texto = str(texto).lower().replace('_', ' ')
        return unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')

    def cargar_json_preguntas(self):
        posibles_rutas = [
            os.path.join(DIR_BASE, "formulario_vacunacion.json"),
            os.path.join(os.getcwd(), "formulario_vacunacion.json"),
            os.path.join(DIR_BASE, "PAGINA", "formulario_vacunacion.json")
        ]
        ruta_json = None
        for r in posibles_rutas:
            if os.path.exists(r): ruta_json = r; break

        if not ruta_json:
            self.logger.info("⚠️ Archivo 'formulario_vacunacion.json' omitido. Usando nombres nativos.")
            return

        try:
            with open(ruta_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
            inputs = data.get('data', {}).get('form', {}).get('inputs', [])

            def extraer_preguntas(elementos):
                for el in elementos:
                    q_sucia = el.get('question', '').strip()
                    q_limpia = re.sub(r'<[^>]+>', '', q_sucia).strip()
                    if q_limpia:
                        clave_limpia = self._limpiar_texto(q_limpia)
                        self.mapeo_automatico[clave_limpia] = q_limpia
                    if 'group' in el and isinstance(el['group'], list):
                        extraer_preguntas(el['group'])

            extraer_preguntas(inputs)
            self.logger.info("✅ Mapeador estructural JSON cargado con éxito.")
        except Exception as e:
            self.logger.info(f"⚠️ Error cargando JSON de preguntas: {e}")

    def encontrar_mejor_coincidencia(self, col_db: str) -> str:
        col_db_normalizada = str(col_db).strip().lower()
        if col_db_normalizada in self.mapeo_exacto: return self.mapeo_exacto[col_db_normalizada]

        col_sin_prefijo = re.sub(r'^[\d_]+', '', str(col_db))
        col_busqueda = self._limpiar_texto(col_sin_prefijo)

        if col_busqueda in self.mapeo_automatico:
            return self.mapeo_automatico[col_busqueda]

        for cleaned_preg, orig_preg in self.mapeo_automatico.items():
            if col_busqueda in cleaned_preg or cleaned_preg in col_busqueda:
                return orig_preg

        return col_sin_prefijo.replace('_', ' ').title()

    def Tanner_clean_cells(self, df_hoja: pd.DataFrame) -> pd.DataFrame:
        if df_hoja.empty: return df_hoja
        df_limpio = df_hoja.copy()
        valores_viciosos = ['None', 'nan', 'NaN', 'NULL', 'null', '']

        for col in df_limpio.columns:
            if pd.api.types.is_object_dtype(df_limpio[col]):
                df_limpio[col] = df_limpio[col].astype(str).str.strip().replace(valores_viciosos, pd.NA)
            else:
                df_limpio[col] = df_limpio[col].replace(valores_viciosos, pd.NA)

        df_limpio = df_limpio.dropna(axis=1, how='all')
        if df_limpio.shape[1] == 0: return df_hoja

        # CORRECCIÓN DEFINITIVA DE DUPLICADOS: Renombramiento con contador (1), (2)...
        nuevos_nombres = []
        vistos = {}
        for col in df_limpio.columns:
            nuevo_nombre = self.encontrar_mejor_coincidencia(col)
            if nuevo_nombre in vistos:
                vistos[nuevo_nombre] += 1
                nuevo_nombre = f"{nuevo_nombre} ({vistos[nuevo_nombre]})"
            else:
                vistos[nuevo_nombre] = 0
            nuevos_nombres.append(nuevo_nombre)

        df_limpio.columns = nuevos_nombres
        return df_limpio

    def reportar(self, df, hoja):
        self.logger.info(f"📊 Hoja '{hoja}': {len(df)} registros procesados.")
        if df.empty: return

        vacs = {'Fiebre Amarilla': ['fiebre amarilla', 'amarilla'], 'Influenza': ['influenza', 'cepa'],
                'Hepatitis A': ['hepatitis a'], 'Hepatitis B': ['hepatitis b'], 'VPH': ['vph', 'papiloma'],
                'COVID-19': ['covid', 'sars'], 'Neumococo': ['neumococo'], 'Rotavirus': ['rotavirus'],
                'Polio': ['polio', 'vop', 'vip'], 'Pentavalente': ['pentavalente', 'penta'],
                'Hexavalente': ['hexavalente', 'hexa'], 'DPT': ['dpt'], 'BCG': ['bcg', 'tuberculosis'],
                'Triple Viral (SRP)': ['triple viral', 'srp', 'sarampion'], 'Varicela': ['varicela'],
                'Toxoide Td': ['toxoide', 'tetano', 'td']}
        c = Counter()

        cols = [col for col in df.columns if
                not any(x in str(col).lower() for x in ['motivo', 'no aplic', 'pendient', 'proxima'])]
        if not cols:
            self.logger.info("-" * 40)
            return

        df_str = df[cols].fillna('').astype(str).apply(lambda x: x.str.lower().str.strip())
        valores_no = {'none', 'nan', 'null', '2. no', 'no', '0', 'false', ''}

        celdas_validas = ~df_str.isin(valores_no)
        df_clean_text = df_str.copy()

        # Como ya garantizamos columnas únicas, este loop es 100% estable
        for col in df_clean_text.columns:
            df_clean_text.loc[~celdas_validas[col], col] = ''

        joined_rows = df_clean_text.agg(' '.join, axis=1)

        for k, keywords in vacs.items():
            pattern = '|'.join([re.escape(kw) for kw in keywords])

            cols_vac = [col for col in cols if any(kw in str(col).lower() for kw in keywords)]
            match_col = pd.Series(False, index=df.index)
            if cols_vac:
                match_col = celdas_validas[cols_vac].any(axis=1)

            match_text = joined_rows.str.contains(pattern, regex=True)

            total_p = (match_col | match_text).sum()
            if total_p > 0:
                c[k] = total_p

        if c:
            self.logger.info(f"   💉 Vacunas identificadas en esta sección:")
            for v, qty in c.most_common(): self.logger.info(f"      ➤ {v}: {qty} pacientes")
        self.logger.info("-" * 40)

    def generar(self, f_ini, f_fin):
        self.cargar_json_preguntas()
        self.logger.info("Extrayendo datos de PostgreSQL...")
        df = pd.read_sql('SELECT * FROM public."vacunacion_aps_2026"', engine)

        if df.empty: raise Exception("No hay datos en la tabla.")
        if f_ini or f_fin:
            col_f = next((c for c in df.columns if 'created_at' in c.lower()), None)
            if col_f:
                fechas = pd.to_datetime(df[col_f], errors='coerce')
                mask = pd.Series(True, index=df.index)
                if f_ini: mask &= (fechas >= pd.to_datetime(f_ini))
                if f_fin: mask &= (fechas <= pd.to_datetime(f_fin) + pd.Timedelta(days=1, seconds=-1))
                df = df[mask]

        self.logger.info(f"Registros totales filtrados a exportar: {len(df)}")
        if df.empty: raise Exception("No hay registros en el rango de fechas seleccionado.")

        for col in df.columns:
            if any(x in col.lower() for x in ['fecha', 'created_at', 'uploaded_at']):
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%d/%m/%Y')

        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_maestro = self.Tanner_clean_cells(df.copy())
            df_maestro.to_excel(writer, sheet_name='Consolidado General', index=False)
            self.reportar(df_maestro, 'Consolidado General')

            col_t = next((c for c in df.columns if "tipo_de_vacunaci" in str(c).lower()), None)
            if col_t:
                df['__tipo_norm__'] = df[col_t].astype(str).str.strip().str.lower()
                categorias = [('recién nacidos', 'Recién Nacidos'), ('niños y niñas', 'Niños y Niñas'),
                              ('adultos', 'Adultos')]

                for val_norm, sheet_name in categorias:
                    df_cat = df[df['__tipo_norm__'] == val_norm].drop(columns=['__tipo_norm__'], errors='ignore')
                    if not df_cat.empty:
                        d = self.Tanner_clean_cells(df_cat)
                        if d.shape[1] > 0: d.to_excel(writer, sheet_name=sheet_name, index=False); self.reportar(d,
                                                                                                                 sheet_name)

                df_otros = df[~df['__tipo_norm__'].isin(['recién nacidos', 'niños y niñas', 'adultos'])].drop(
                    columns=['__tipo_norm__'], errors='ignore')
                if not df_otros.empty:
                    d_otros = self.Tanner_clean_cells(df_otros)
                    if d_otros.shape[1] > 0: d_otros.to_excel(writer, sheet_name='Sin Clasificar',
                                                              index=False); self.reportar(d_otros, 'Sin Clasificar')
        self.logger.info("✅ Archivo Excel estructurado con éxito.")
        output.seek(0)
        return output


if __name__ == "__main__":
    port = int(os.getenv("PORT_VACUNACION", 5002))
    app.run(host="0.0.0.0", port=port, debug=False)
