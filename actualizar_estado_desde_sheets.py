# actualizar_estado_desde_sheets.py
import json
import logging
from datetime import datetime
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials

# ═════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═════════════════════════════════════════════════════════════

BASE_PATH = Path(r"C:\Users\cmarroquin\Music\RuntPro")
GOOGLE_CREDS = BASE_PATH / "prueba-de-gmail-486215-345473339c47.json"
ESTADO_FILE = BASE_PATH / "Escritura_Runt_principal" / "estado_runt.json"
LOGS_FOLDER = BASE_PATH / "Escritura_Runt_principal"

# IDs de los spreadsheets
DESTINO_SPREADSHEET_ID = "1vs414iH3QVeLoTcY2CExg4kD9eCkXZRRfax_WTlUXPk"
ORIGEN_SPREADSHEET_ID = "1saIDw37nd-rnzZvvKjxUQP41LhXJvSiayYgFRR78N7o"

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGS_FOLDER / "actualizar_estado.log", encoding='utf-8')
    ]
)

def conectar_google_sheets():
    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(str(GOOGLE_CREDS), scopes=SCOPES)
    return gspread.authorize(creds)

def leer_placas_desde_datos_runt(client):
    """Lee todas las placas de la hoja 'Datos Runt'"""
    try:
        sheet = client.open_by_key(DESTINO_SPREADSHEET_ID)
        worksheet = sheet.worksheet("Datos Runt")
        todas_filas = worksheet.get_all_values()
        
        placas_exitosas = set()
        for i in range(1, len(todas_filas)):
            fila = todas_filas[i]
            if len(fila) > 2:
                placa = str(fila[2]).strip().upper()
                if placa and placa != "PLACA" and placa != "":
                    placas_exitosas.add(placa)
        
        logging.info(f"📊 Leídas {len(placas_exitosas)} placas desde 'Datos Runt'")
        return placas_exitosas
    except Exception as e:
        logging.error(f"❌ Error leyendo Datos Runt: {e}")
        return set()

def leer_placas_desde_resultados(client):
    """Lee placas con estado 'Funcionó' desde la hoja 'Resultados'"""
    try:
        sheet = client.open_by_key(DESTINO_SPREADSHEET_ID)
        worksheet = sheet.worksheet("Resultados")
        todas_filas = worksheet.get_all_values()
        
        placas_exitosas = set()
        for i in range(1, len(todas_filas)):
            fila = todas_filas[i]
            if len(fila) >= 5:
                placa = str(fila[2]).strip().upper()
                estado = str(fila[4]).strip()
                if placa and placa != "PLACA" and "Funcionó" in estado:
                    placas_exitosas.add(placa)
        
        logging.info(f"📊 Leídas {len(placas_exitosas)} placas 'Funcionó' desde 'Resultados'")
        return placas_exitosas
    except Exception as e:
        logging.error(f"❌ Error leyendo Resultados: {e}")
        return set()

def leer_datos_origen_para_placas(client, placas):
    """Lee las cédulas de origen para las placas dadas"""
    try:
        sheet = client.open_by_key(ORIGEN_SPREADSHEET_ID)
        nombres_sheets = ["Motos 0_5", "Motos 6_10", "Motos 11_15", "Motos 16_25"]
        
        datos_placas = {}
        
        for nombre_sheet in nombres_sheets:
            try:
                worksheet = sheet.worksheet(nombre_sheet)
                cedulas_asociado = worksheet.col_values(2)
                cedulas_propietario = worksheet.col_values(4)
                placas_sheet = worksheet.col_values(6)
                
                for i in range(1, min(len(placas_sheet), len(cedulas_asociado))):
                    placa = str(placas_sheet[i]).strip().upper()
                    if placa in placas:
                        if placa not in datos_placas:
                            datos_placas[placa] = {
                                "cedula_asociado": str(cedulas_asociado[i]).strip(),
                                "cedula_propietario": str(cedulas_propietario[i]).strip(),
                                "sheet": nombre_sheet,
                                "fila": i + 1
                            }
            except Exception as e:
                logging.warning(f"⚠️ Error en {nombre_sheet}: {e}")
        
        return datos_placas
    except Exception as e:
        logging.error(f"❌ Error leyendo origen: {e}")
        return {}

def actualizar_estado_json(placas_exitosas, datos_origen):
    """Actualiza el archivo estado_runt.json con las placas exitosas"""
    
    # Cargar estado actual si existe
    estado_actual = {}
    if ESTADO_FILE.exists():
        try:
            with open(ESTADO_FILE, "r", encoding="utf-8") as f:
                estado_actual = json.load(f)
        except:
            estado_actual = {"resumen": {"placas_procesadas": {}}, "historial_completo": []}
    
    # Asegurar estructura
    if "resumen" not in estado_actual:
        estado_actual["resumen"] = {"placas_procesadas": {}, "ultima_actualizacion": None}
    if "placas_procesadas" not in estado_actual["resumen"]:
        estado_actual["resumen"]["placas_procesadas"] = {}
    
    # Contar cuántas nuevas se agregan
    nuevas = 0
    actualizadas = 0
    
    for placa in placas_exitosas:
        estado_anterior = estado_actual["resumen"]["placas_procesadas"].get(placa, "NO_EXISTE")
        
        if estado_anterior != "Exitoso":
            estado_actual["resumen"]["placas_procesadas"][placa] = "Exitoso"
            nuevas += 1
            
            # Si tenemos datos de origen, agregar al historial
            if placa in datos_origen:
                info = datos_origen[placa]
                nuevo_registro = {
                    "timestamp": datetime.now().isoformat(),
                    "cedula": info["cedula_asociado"],
                    "placa": placa,
                    "status": "Exitoso (actualizado desde sheets)",
                    "datos_vehiculo": {},
                    "datos_soat": [],
                    "datos_tecnica": []
                }
                if "historial_completo" not in estado_actual:
                    estado_actual["historial_completo"] = []
                estado_actual["historial_completo"].append(nuevo_registro)
    
    # Actualizar metadatos
    estado_actual["resumen"]["ultima_actualizacion"] = datetime.now().isoformat()
    estado_actual["last_execution"] = datetime.now().isoformat()
    
    # Guardar
    with open(ESTADO_FILE, "w", encoding="utf-8") as f:
        json.dump(estado_actual, f, indent=2, ensure_ascii=False)
    
    logging.info(f"\n✅ Estado actualizado:")
    logging.info(f"   📊 Total de placas en JSON: {len(estado_actual['resumen']['placas_procesadas'])}")
    logging.info(f"   ✨ Nuevas placas agregadas: {nuevas}")
    logging.info(f"   🔄 Placas actualizadas: {actualizadas}")
    
    return estado_actual

def main():
    logging.info("="*70)
    logging.info("🔄 ACTUALIZANDO estado_runt.json desde Google Sheets")
    logging.info("="*70)
    
    try:
        client = conectar_google_sheets()
        logging.info("✅ Conexión establecida")
        
        # Paso 1: Leer placas exitosas
        logging.info("\n📥 Leyendo placas desde 'Datos Runt'...")
        placas_datos_runt = leer_placas_desde_datos_runt(client)
        
        logging.info("\n📥 Leyendo placas 'Funcionó' desde 'Resultados'...")
        placas_resultados = leer_placas_desde_resultados(client)
        
        # Unir ambas fuentes
        todas_placas_exitosas = placas_datos_runt.union(placas_resultados)
        logging.info(f"\n📊 TOTAL DE PLACAS EXITOSAS: {len(todas_placas_exitosas)}")
        
        # Paso 2: Obtener datos de origen para estas placas
        logging.info("\n🔍 Buscando datos de origen para las placas...")
        datos_origen = leer_datos_origen_para_placas(client, todas_placas_exitosas)
        logging.info(f"   ✅ Datos encontrados para {len(datos_origen)} placas")
        
        # Paso 3: Actualizar JSON
        estado_actualizado = actualizar_estado_json(todas_placas_exitosas, datos_origen)
        
        # Mostrar las últimas placas agregadas
        placas_json = estado_actualizado["resumen"]["placas_procesadas"]
        ultimas_placas = list(placas_json.keys())[-20:]
        
        logging.info(f"\n📋 ÚLTIMAS 20 PLACAS EN EL JSON:")
        for placa in ultimas_placas:
            logging.info(f"   - {placa}: {placas_json[placa]}")
        
        logging.info("\n" + "="*70)
        logging.info("✅ PROCESO COMPLETADO")
        logging.info("="*70)
        
    except Exception as e:
        logging.error(f"❌ Error: {e}", exc_info=True)

if __name__ == "__main__":
    main()