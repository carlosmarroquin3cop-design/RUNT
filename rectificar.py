# rectificar.py
import logging
import os
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials

# ═════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE RUTAS
# ═════════════════════════════════════════════════════════════

BASE_PATH = Path(r"C:\Users\cmarroquin\Music\RuntPro")
LOGS_RECTIFICAR = BASE_PATH / "rectificar"
LOGS_RECTIFICAR.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDS = BASE_PATH / "prueba-de-gmail-486215-345473339c47.json"

# IDs de los spreadsheets
ORIGEN_SPREADSHEET_ID = "1saIDw37nd-rnzZvvKjxUQP41LhXJvSiayYgFRR78N7o"
DESTINO_SPREADSHEET_ID = "1vs414iH3QVeLoTcY2CExg4kD9eCkXZRRfax_WTlUXPk"

# ═════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE LOGS INDEPENDIENTES
# ═════════════════════════════════════════════════════════════

def configurar_logs_rectificar():
    logger = logging.getLogger('rectificar')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    fecha = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_handler = logging.FileHandler(
        str(LOGS_RECTIFICAR / f"rectificacion_{fecha}.log"),
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = configurar_logs_rectificar()

# ═════════════════════════════════════════════════════════════
# CONEXIÓN A GOOGLE SHEETS
# ═════════════════════════════════════════════════════════════

def conectar_google_sheets():
    SCOPES = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    creds = Credentials.from_service_account_file(
        str(GOOGLE_CREDS),
        scopes=SCOPES
    )
    
    return gspread.authorize(creds)

# ═════════════════════════════════════════════════════════════
# 1. LEER HOJA "RESULTADOS"
# ═════════════════════════════════════════════════════════════

def leer_hoja_resultados(client):
    """Lee todas las placas de la hoja 'Resultados'"""
    
    logger.info("\n" + "="*80)
    logger.info("📥 [DESTINO] Leyendo hoja: 'Resultados'")
    logger.info("="*80)
    
    try:
        sheet = client.open_by_key(DESTINO_SPREADSHEET_ID)
        worksheet = sheet.worksheet("Resultados")
        
        todas_filas = worksheet.get_all_values()
        
        if len(todas_filas) <= 1:
            logger.warning("   Hoja 'Resultados' está vacía o solo tiene encabezados")
            return set(), []
        
        logger.info(f"   Total de filas (incluyendo encabezado): {len(todas_filas)}")
        logger.info(f"   Filas de datos: {len(todas_filas) - 1}")
        
        placas_resultados = set()
        detalle_resultados = []
        
        # Columna C (índice 2) es la placa
        # Columna E (índice 4) es el estado (Funcionó/Falló)
        
        for i in range(1, len(todas_filas)):
            fila = todas_filas[i]
            if len(fila) > 2:
                placa = str(fila[2]).strip().upper()
                estado = str(fila[4]).strip() if len(fila) > 4 else "Desconocido"
                
                if placa and placa != "" and placa != "PLACA":
                    placas_resultados.add(placa)
                    detalle_resultados.append({
                        "placa": placa,
                        "estado": estado,
                        "fila": i + 1
                    })
        
        logger.info(f"   ✅ Placas encontradas: {len(placas_resultados)}")
        
        funcionaron = sum(1 for d in detalle_resultados if "Funcionó" in d["estado"])
        fallaron = sum(1 for d in detalle_resultados if "Falló" in d["estado"])
        logger.info(f"   📊 Desglose: Funcionaron={funcionaron}, Fallaron={fallaron}")
        
        if placas_resultados:
            muestra = list(placas_resultados)[:10]
            logger.info(f"   📋 Muestra: {muestra}")
        
        return placas_resultados, detalle_resultados
        
    except gspread.WorksheetNotFound:
        logger.error("❌ Hoja 'Resultados' no encontrada")
        return set(), []
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return set(), []

# ═════════════════════════════════════════════════════════════
# 2. LEER HOJA "DATOS RUNT"
# ═════════════════════════════════════════════════════════════

def leer_hoja_datos_runt(client):
    """Lee todas las placas de la hoja 'Datos Runt'"""
    
    logger.info("\n" + "="*80)
    logger.info("📥 [DESTINO] Leyendo hoja: 'Datos Runt'")
    logger.info("="*80)
    
    try:
        sheet = client.open_by_key(DESTINO_SPREADSHEET_ID)
        worksheet = sheet.worksheet("Datos Runt")
        
        todas_filas = worksheet.get_all_values()
        
        if len(todas_filas) <= 1:
            logger.warning("   Hoja 'Datos Runt' está vacía o solo tiene encabezados")
            return set(), []
        
        logger.info(f"   Total de filas (incluyendo encabezado): {len(todas_filas)}")
        logger.info(f"   Filas de datos: {len(todas_filas) - 1}")
        
        placas_datos_runt = set()
        detalle_datos_runt = []
        
        # Columna C (índice 2) es la placa
        for i in range(1, len(todas_filas)):
            fila = todas_filas[i]
            if len(fila) > 2:
                placa = str(fila[2]).strip().upper()
                if placa and placa != "" and placa != "PLACA":
                    placas_datos_runt.add(placa)
                    detalle_datos_runt.append({
                        "placa": placa,
                        "fila": i + 1
                    })
        
        logger.info(f"   ✅ Placas encontradas: {len(placas_datos_runt)}")
        
        if placas_datos_runt:
            muestra = list(placas_datos_runt)[:10]
            logger.info(f"   📋 Muestra: {muestra}")
        
        return placas_datos_runt, detalle_datos_runt
        
    except gspread.WorksheetNotFound:
        logger.error("❌ Hoja 'Datos Runt' no encontrada")
        return set(), []
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return set(), []

# ═════════════════════════════════════════════════════════════
# 3. LEER HOJA "DATOS VEHICULO"
# ═════════════════════════════════════════════════════════════

def leer_hoja_datos_vehiculo(client):
    """Lee todas las placas de la hoja 'Datos Vehiculo'"""
    
    logger.info("\n" + "="*80)
    logger.info("📥 [DESTINO] Leyendo hoja: 'Datos Vehiculo'")
    logger.info("="*80)
    
    try:
        sheet = client.open_by_key(DESTINO_SPREADSHEET_ID)
        worksheet = sheet.worksheet("Datos Vehiculo")
        
        todas_filas = worksheet.get_all_values()
        
        if len(todas_filas) <= 1:
            logger.warning("   Hoja 'Datos Vehiculo' está vacía o solo tiene encabezados")
            return set(), []
        
        logger.info(f"   Total de filas (incluyendo encabezado): {len(todas_filas)}")
        logger.info(f"   Filas de datos: {len(todas_filas) - 1}")
        
        placas_datos_vehiculo = set()
        detalle_datos_vehiculo = []
        
        # Columna A (índice 0) es la placa
        for i in range(1, len(todas_filas)):
            fila = todas_filas[i]
            if len(fila) > 0:
                placa = str(fila[0]).strip().upper()
                if placa and placa != "" and placa != "PLACA":
                    placas_datos_vehiculo.add(placa)
                    detalle_datos_vehiculo.append({
                        "placa": placa,
                        "fila": i + 1
                    })
        
        logger.info(f"   ✅ Placas encontradas: {len(placas_datos_vehiculo)}")
        
        if placas_datos_vehiculo:
            muestra = list(placas_datos_vehiculo)[:10]
            logger.info(f"   📋 Muestra: {muestra}")
        
        return placas_datos_vehiculo, detalle_datos_vehiculo
        
    except gspread.WorksheetNotFound:
        logger.warning("⚠️ Hoja 'Datos Vehiculo' no encontrada (puede no existir aún)")
        return set(), []
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return set(), []

# ═════════════════════════════════════════════════════════════
# 4. LEER HOJA "SIN ASOCIADOS"
# ═════════════════════════════════════════════════════════════

def leer_hoja_sin_asociados(client):
    """Lee todas las placas de la hoja 'Sin Asociados'"""
    
    logger.info("\n" + "="*80)
    logger.info("📥 [DESTINO] Leyendo hoja: 'Sin Asociados'")
    logger.info("="*80)
    
    try:
        sheet = client.open_by_key(DESTINO_SPREADSHEET_ID)
        worksheet = sheet.worksheet("Sin Asociados")
        
        todas_filas = worksheet.get_all_values()
        
        if len(todas_filas) <= 1:
            logger.warning("   Hoja 'Sin Asociados' está vacía o solo tiene encabezados")
            return set(), []
        
        logger.info(f"   Total de filas (incluyendo encabezado): {len(todas_filas)}")
        logger.info(f"   Filas de datos: {len(todas_filas) - 1}")
        
        placas_sin_asociados = set()
        detalle_sin_asociados = []
        
        # Buscar columna de placa (probablemente C)
        if len(todas_filas) > 0:
            encabezados = [str(c).lower() for c in todas_filas[0]]
            columna_placa = None
            for idx, enc in enumerate(encabezados):
                if "placa" in enc:
                    columna_placa = idx
                    break
            
            if columna_placa is None:
                columna_placa = 2  # Default columna C
            
            for i in range(1, len(todas_filas)):
                fila = todas_filas[i]
                if len(fila) > columna_placa:
                    placa = str(fila[columna_placa]).strip().upper()
                    if placa and placa != "" and placa != "PLACA":
                        placas_sin_asociados.add(placa)
                        detalle_sin_asociados.append({
                            "placa": placa,
                            "fila": i + 1
                        })
        
        logger.info(f"   ✅ Placas encontradas: {len(placas_sin_asociados)}")
        
        if placas_sin_asociados:
            muestra = list(placas_sin_asociados)[:10]
            logger.info(f"   📋 Muestra: {muestra}")
        
        return placas_sin_asociados, detalle_sin_asociados
        
    except gspread.WorksheetNotFound:
        logger.warning("⚠️ Hoja 'Sin Asociados' no encontrada (puede no existir aún)")
        return set(), []
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return set(), []

# ═════════════════════════════════════════════════════════════
# 5. LEER HOJAS DE ORIGEN (Motos 0_5, 6_10, 11_15, 16_25)
# ═════════════════════════════════════════════════════════════

def leer_hojas_origen(client):
    """Lee todas las placas de las hojas de origen"""
    
    logger.info("\n" + "="*80)
    logger.info("📥 [ORIGEN] Leyendo hojas: Motos 0_5, Motos 6_10, Motos 11_15, Motos 16_25")
    logger.info("="*80)
    
    try:
        sheet = client.open_by_key(ORIGEN_SPREADSHEET_ID)
        
        nombres_sheets = ["Motos 0_5", "Motos 6_10", "Motos 11_15", "Motos 16_25"]
        
        placas_origen = set()
        detalle_origen = []
        
        total_registros = 0
        
        for nombre_sheet in nombres_sheets:
            try:
                worksheet = sheet.worksheet(nombre_sheet)
                todas_filas = worksheet.get_all_values()
                
                if len(todas_filas) <= 1:
                    logger.warning(f"   ⚠️  Sheet '{nombre_sheet}' vacía")
                    continue
                
                logger.info(f"\n📄 Leyendo sheet: {nombre_sheet}")
                logger.info(f"   Total de filas: {len(todas_filas)}")
                
                # Obtener columnas
                cedulas_asociado = worksheet.col_values(2)   # Columna B
                cedulas_propietario = worksheet.col_values(4) # Columna D
                placas = worksheet.col_values(6)              # Columna F
                
                registros_sheet = 0
                
                for i in range(1, min(len(cedulas_asociado), len(cedulas_propietario), len(placas))):
                    cedula_asoc = str(cedulas_asociado[i]).strip()
                    cedula_prop = str(cedulas_propietario[i]).strip()
                    placa = str(placas[i]).strip().upper()
                    
                    if not placa or placa == "NAN" or placa == "":
                        continue
                    
                    placas_origen.add(placa)
                    detalle_origen.append({
                        "placa": placa,
                        "sheet": nombre_sheet,
                        "fila": i + 1,
                        "cedula_asociado": cedula_asoc if cedula_asoc and cedula_asoc.lower() != "nan" else "SIN_DATO",
                        "cedula_propietario": cedula_prop if cedula_prop and cedula_prop.lower() != "nan" else "SIN_DATO"
                    })
                    registros_sheet += 1
                    total_registros += 1
                
                logger.info(f"   ✅ Registros en '{nombre_sheet}': {registros_sheet}")
                
                # Mostrar primeras 3 filas como ejemplo
                ejemplos = [d for d in detalle_origen if d["sheet"] == nombre_sheet][:3]
                for ex in ejemplos:
                    logger.info(f"      Ejemplo: {ex['placa']} | Céd.Asoc: {ex['cedula_asociado'][:12]}")
                
            except gspread.WorksheetNotFound:
                logger.warning(f"   ⚠️  Sheet '{nombre_sheet}' no encontrada")
            except Exception as e:
                logger.error(f"   ❌ Error en '{nombre_sheet}': {e}")
        
        logger.info(f"\n📊 TOTAL ORIGEN:")
        logger.info(f"   Placas únicas: {len(placas_origen)}")
        logger.info(f"   Total registros (con duplicados entre sheets): {total_registros}")
        
        return placas_origen, detalle_origen
        
    except Exception as e:
        logger.error(f"❌ Error leyendo hojas de origen: {e}")
        return set(), []

# ═════════════════════════════════════════════════════════════
# NIVEL 1: COMPARACIÓN INTERNA (Resultados vs Datos Runt)
# ═════════════════════════════════════════════════════════════

def nivel1_comparacion_interna(placas_resultados, detalle_resultados, placas_datos_runt):
    """
    NIVEL 1: Compara Resultados vs Datos Runt (dentro del destino)
    Para ver consistencia interna
    """
    
    logger.info("\n" + "="*80)
    logger.info("🔍 NIVEL 1: COMPARACIÓN INTERNA - Resultados vs Datos Runt")
    logger.info("="*80)
    
    # Placas en Resultados pero NO en Datos Runt
    solo_en_resultados = placas_resultados - placas_datos_runt
    
    # Placas en Datos Runt pero NO en Resultados
    solo_en_datos_runt = placas_datos_runt - placas_resultados
    
    # Placas en ambas
    en_ambas = placas_resultados & placas_datos_runt
    
    logger.info(f"\n📊 RESULTADOS:")
    logger.info(f"   ✅ En AMBAS (consistente): {len(en_ambas)} placas")
    logger.info(f"   ⚠️  SOLO en Resultados: {len(solo_en_resultados)} placas")
    logger.info(f"   ⚠️  SOLO en Datos Runt: {len(solo_en_datos_runt)} placas")
    
    # ═══ DETALLE DE SOLO EN RESULTADOS ═══
    if solo_en_resultados:
        logger.info("\n" + "-"*80)
        logger.info(f"🔴 PLACAS EN 'RESULTADOS' PERO NO EN 'DATOS RUNT' ({len(solo_en_resultados)})")
        logger.info("-"*80)
        
        detalle = []
        for placa in sorted(solo_en_resultados):
            info = next((d for d in detalle_resultados if d["placa"] == placa), None)
            estado = info["estado"] if info else "Desconocido"
            detalle.append({"placa": placa, "estado": estado})
            logger.info(f"      {placa} - Estado: {estado}")
        
        # Guardar archivo
        archivo = LOGS_RECTIFICAR / f"nivel1_solo_en_resultados_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(archivo, 'w', encoding='utf-8') as f:
            f.write("NIVEL 1 - PLACAS EN 'RESULTADOS' PERO NO EN 'DATOS RUNT'\n")
            f.write("="*60 + "\n\n")
            for item in detalle:
                f.write(f"{item['placa']} | Estado: {item['estado']}\n")
        logger.info(f"💾 Guardado en: {archivo}")
    
    # ═══ DETALLE DE SOLO EN DATOS RUNT ═══
    if solo_en_datos_runt:
        logger.info("\n" + "-"*80)
        logger.info(f"🟡 PLACAS EN 'DATOS RUNT' PERO NO EN 'RESULTADOS' ({len(solo_en_datos_runt)})")
        logger.info("-"*80)
        
        for placa in sorted(solo_en_datos_runt)[:30]:
            logger.info(f"      {placa}")
        if len(solo_en_datos_runt) > 30:
            logger.info(f"      ... y {len(solo_en_datos_runt) - 30} más")
        
        archivo = LOGS_RECTIFICAR / f"nivel1_solo_en_datos_runt_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(archivo, 'w', encoding='utf-8') as f:
            f.write("NIVEL 1 - PLACAS EN 'DATOS RUNT' PERO NO EN 'RESULTADOS'\n")
            f.write("="*60 + "\n\n")
            for placa in sorted(solo_en_datos_runt):
                f.write(f"{placa}\n")
        logger.info(f"💾 Guardado en: {archivo}")
    
    return {
        "solo_en_resultados": solo_en_resultados,
        "solo_en_datos_runt": solo_en_datos_runt,
        "en_ambas": en_ambas
    }

# ═════════════════════════════════════════════════════════════
# NIVEL 2: COMPARACIÓN GLOBAL (TODAS LAS HOJAS DESTINO vs ORIGEN)
# ═════════════════════════════════════════════════════════════

def nivel2_comparacion_global(placas_origen, detalle_origen, todas_placas_destino, detalle_por_hoja_destino):
    """
    NIVEL 2: Compara TODAS las hojas de destino contra las hojas de origen
    Identifica qué placas de origen NO aparecen en NINGUNA hoja de destino
    """
    
    logger.info("\n" + "="*80)
    logger.info("🌍 NIVEL 2: COMPARACIÓN GLOBAL - Destino vs Origen")
    logger.info("="*80)
    
    # Placas que están en origen pero NO en NINGUNA hoja de destino
    placas_faltantes_global = placas_origen - todas_placas_destino
    
    # Placas que están en destino pero NO en origen (sobrantes)
    placas_sobrantes = todas_placas_destino - placas_origen
    
    # Placas que están en ambas
    en_ambas = placas_origen & todas_placas_destino
    
    logger.info(f"\n📊 ESTADÍSTICAS GLOBALES:")
    logger.info(f"   📌 ORIGEN: {len(placas_origen)} placas únicas")
    logger.info(f"   📌 DESTINO (TODAS las hojas): {len(todas_placas_destino)} placas únicas")
    logger.info(f"\n   ✅ YA PROCESADAS (en destino): {len(en_ambas)} placas")
    logger.info(f"   ❌ FALTANTES (NO en destino): {len(placas_faltantes_global)} placas")
    logger.info(f"   ⚠️  SOBRANTES (en destino pero NO en origen): {len(placas_sobrantes)} placas")
    
    # Porcentaje de completitud
    if len(placas_origen) > 0:
        porcentaje = (len(en_ambas) / len(placas_origen)) * 100
        logger.info(f"\n📈 PORCENTAJE COMPLETADO: {porcentaje:.2f}% ({len(en_ambas)}/{len(placas_origen)})")
    
    # ═══ DETALLE DE PLACAS FALTANTES ═══
    if placas_faltantes_global:
        logger.info("\n" + "-"*80)
        logger.info(f"❌ PLACAS FALTANTES (en origen pero NO en NINGUNA hoja de destino) ({len(placas_faltantes_global)})")
        logger.info("-"*80)
        
        # Enriquecer con datos de origen
        detalle_faltantes = []
        for placa in sorted(placas_faltantes_global):
            info_origen = next((d for d in detalle_origen if d["placa"] == placa), None)
            if info_origen:
                detalle_faltantes.append({
                    "placa": placa,
                    "sheet_origen": info_origen["sheet"],
                    "fila_origen": info_origen["fila"],
                    "cedula_asociado": info_origen["cedula_asociado"],
                    "cedula_propietario": info_origen["cedula_propietario"]
                })
                logger.info(f"      {placa} | Sheet: {info_origen['sheet']} | Fila: {info_origen['fila']} | Céd.Asoc: {info_origen['cedula_asociado'][:12]}")
            else:
                detalle_faltantes.append({
                    "placa": placa,
                    "sheet_origen": "NO_ENCONTRADA",
                    "fila_origen": "N/A",
                    "cedula_asociado": "N/A",
                    "cedula_propietario": "N/A"
                })
                logger.warning(f"      {placa} | ¡NO ENCONTRADA EN ORIGEN! (revisar)")
        
        # Guardar archivo con detalle completo
        archivo_detalle = LOGS_RECTIFICAR / f"nivel2_placas_faltantes_detalle_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(archivo_detalle, 'w', encoding='utf-8') as f:
            f.write("NIVEL 2 - PLACAS FALTANTES (en origen pero NO en destino)\n")
            f.write("="*80 + "\n\n")
            f.write(f"Total: {len(detalle_faltantes)} placas\n\n")
            for item in detalle_faltantes:
                f.write(f"{item['placa']} | Sheet: {item['sheet_origen']} | Fila: {item['fila_origen']} | Céd.Asoc: {item['cedula_asociado']} | Céd.Prop: {item['cedula_propietario']}\n")
        
        # Guardar archivo solo con las placas (para copiar/pegar)
        archivo_placas = LOGS_RECTIFICAR / f"nivel2_placas_faltantes_solo_lista_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(archivo_placas, 'w', encoding='utf-8') as f:
            for item in detalle_faltantes:
                f.write(f"{item['placa']}\n")
        
        logger.info(f"\n💾 Detalle completo guardado en: {archivo_detalle}")
        logger.info(f"💾 Lista de placas guardada en: {archivo_placas}")
        
        # Agrupar por sheet de origen
        faltantes_por_sheet = defaultdict(list)
        for item in detalle_faltantes:
            faltantes_por_sheet[item["sheet_origen"]].append(item)
        
        logger.info("\n📊 RESUMEN POR SHEET DE ORIGEN:")
        for sheet, items in sorted(faltantes_por_sheet.items()):
            logger.info(f"   {sheet}: {len(items)} placas faltantes")
    
    else:
        logger.info("\n" + "-"*80)
        logger.info("✅ ¡TODAS LAS PLACAS DE ORIGEN ESTÁN EN DESTINO!")
        logger.info("-"*80)
    
    # ═══ DETALLE DE PLACAS SOBRANTES ═══
    if placas_sobrantes:
        logger.info("\n" + "-"*80)
        logger.info(f"⚠️  PLACAS SOBRANTES (en destino pero NO en origen) ({len(placas_sobrantes)})")
        logger.info("-"*80)
        
        # Mostrar en qué hoja aparece cada una
        for placa in sorted(placas_sobrantes)[:30]:
            hojas = detalle_por_hoja_destino.get(placa, [])
            logger.info(f"      {placa} -> aparece en: {', '.join(hojas)}")
        if len(placas_sobrantes) > 30:
            logger.info(f"      ... y {len(placas_sobrantes) - 30} más")
        
        archivo = LOGS_RECTIFICAR / f"nivel2_placas_sobrantes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(archivo, 'w', encoding='utf-8') as f:
            f.write("NIVEL 2 - PLACAS SOBRANTES (en destino pero NO en origen)\n")
            f.write("="*60 + "\n\n")
            for placa in sorted(placas_sobrantes):
                hojas = detalle_por_hoja_destino.get(placa, [])
                f.write(f"{placa} | Hojas: {', '.join(hojas)}\n")
        logger.info(f"💾 Guardado en: {archivo}")
    
    return {
        "faltantes": placas_faltantes_global,
        "sobrantes": placas_sobrantes,
        "procesadas": en_ambas,
        "porcentaje": porcentaje if len(placas_origen) > 0 else 0,
        "detalle_faltantes": detalle_faltantes if placas_faltantes_global else []
    }

# ═════════════════════════════════════════════════════════════
# 8. FUNCIÓN PRINCIPAL
# ═════════════════════════════════════════════════════════════

def main():
    """Función principal de rectificación - Dos niveles de comparación"""
    
    logger.info("="*80)
    logger.info("🚀 INICIANDO PROCESO DE RECTIFICACIÓN DE DATOS RUNT")
    logger.info("="*80)
    logger.info(f"📁 Logs guardados en: {LOGS_RECTIFICAR}")
    logger.info(f"🕐 Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        client = conectar_google_sheets()
        logger.info("✅ Conexión a Google Sheets establecida")
        
        # ═══════════════════════════════════════════════════════════
        # LECTURA DE TODAS LAS HOJAS
        # ═══════════════════════════════════════════════════════════
        
        # Leer hojas de destino individuales
        placas_resultados, detalle_resultados = leer_hoja_resultados(client)
        placas_datos_runt, _ = leer_hoja_datos_runt(client)
        placas_datos_vehiculo, _ = leer_hoja_datos_vehiculo(client)
        placas_sin_asociados, _ = leer_hoja_sin_asociados(client)
        
        # Leer hojas de origen
        placas_origen, detalle_origen = leer_hojas_origen(client)
        
        # ═══ UNIR TODAS LAS PLACAS DE DESTINO ═══
        todas_placas_destino = set()
        todas_placas_destino.update(placas_resultados)
        todas_placas_destino.update(placas_datos_runt)
        todas_placas_destino.update(placas_datos_vehiculo)
        todas_placas_destino.update(placas_sin_asociados)
        
        # Detalle de en qué hoja aparece cada placa
        detalle_por_hoja_destino = defaultdict(list)
        for placa in placas_resultados:
            detalle_por_hoja_destino[placa].append("Resultados")
        for placa in placas_datos_runt:
            detalle_por_hoja_destino[placa].append("Datos Runt")
        for placa in placas_datos_vehiculo:
            detalle_por_hoja_destino[placa].append("Datos Vehiculo")
        for placa in placas_sin_asociados:
            detalle_por_hoja_destino[placa].append("Sin Asociados")
        
        logger.info("\n" + "="*80)
        logger.info("📊 RESUMEN DE LECTURA - HOJAS DE DESTINO")
        logger.info("="*80)
        logger.info(f"   Resultados: {len(placas_resultados)} placas")
        logger.info(f"   Datos Runt: {len(placas_datos_runt)} placas")
        logger.info(f"   Datos Vehiculo: {len(placas_datos_vehiculo)} placas")
        logger.info(f"   Sin Asociados: {len(placas_sin_asociados)} placas")
        logger.info(f"   → TOTAL ÚNICAS en destino: {len(todas_placas_destino)} placas")
        
        # ═══════════════════════════════════════════════════════════
        # NIVEL 1: COMPARACIÓN INTERNA (Resultados vs Datos Runt)
        # ═══════════════════════════════════════════════════════════
        
        resultado_nivel1 = nivel1_comparacion_interna(
            placas_resultados,
            detalle_resultados,
            placas_datos_runt
        )
        
        # ═══════════════════════════════════════════════════════════
        # NIVEL 2: COMPARACIÓN GLOBAL (Destino vs Origen)
        # ═══════════════════════════════════════════════════════════
        
        resultado_nivel2 = nivel2_comparacion_global(
            placas_origen,
            detalle_origen,
            todas_placas_destino,
            detalle_por_hoja_destino
        )
        
        # ═══════════════════════════════════════════════════════════
        # REPORTE FINAL EJECUTIVO
        # ═══════════════════════════════════════════════════════════
        
        logger.info("\n" + "="*80)
        logger.info("📋 REPORTE FINAL EJECUTIVO")
        logger.info("="*80)
        
        logger.info("\n🔍 NIVEL 1 - Consistencia interna:")
        logger.info(f"   ✅ En ambas (Resultados + Datos Runt): {len(resultado_nivel1['en_ambas'])}")
        logger.info(f"   ⚠️  Solo en Resultados: {len(resultado_nivel1['solo_en_resultados'])}")
        logger.info(f"   ⚠️  Solo en Datos Runt: {len(resultado_nivel1['solo_en_datos_runt'])}")
        
        logger.info("\n🌍 NIVEL 2 - Cobertura global:")
        logger.info(f"   📌 ORIGEN: {len(placas_origen)} placas únicas")
        logger.info(f"   📌 DESTINO (todas las hojas): {len(todas_placas_destino)} placas únicas")
        logger.info(f"   ✅ YA PROCESADAS: {len(resultado_nivel2['procesadas'])} placas")
        logger.info(f"   ❌ FALTANTES: {len(resultado_nivel2['faltantes'])} placas")
        logger.info(f"   📈 PORCENTAJE COMPLETADO: {resultado_nivel2['porcentaje']:.2f}%")
        
        if resultado_nivel2['faltantes']:
            logger.info(f"\n⚠️  ATENCIÓN: Faltan {len(resultado_nivel2['faltantes'])} placas por procesar")
            logger.info(f"   Revisa los archivos en la carpeta 'rectificar'")
        else:
            logger.info(f"\n🎉 ¡FELICIDADES! Todas las placas de origen están en destino")
        
        # Guardar reporte completo en JSON
        reporte_completo = {
            "fecha": datetime.now().isoformat(),
            "nivel1": {
                "en_ambas": len(resultado_nivel1["en_ambas"]),
                "solo_en_resultados": len(resultado_nivel1["solo_en_resultados"]),
                "solo_en_datos_runt": len(resultado_nivel1["solo_en_datos_runt"]),
                "lista_solo_resultados": list(resultado_nivel1["solo_en_resultados"]),
                "lista_solo_datos_runt": list(resultado_nivel1["solo_en_datos_runt"])
            },
            "nivel2": {
                "total_origen": len(placas_origen),
                "total_destino_unico": len(todas_placas_destino),
                "procesadas": len(resultado_nivel2["procesadas"]),
                "faltantes": len(resultado_nivel2["faltantes"]),
                "sobrantes": len(resultado_nivel2["sobrantes"]),
                "porcentaje": resultado_nivel2["porcentaje"],
                "detalle_faltantes": resultado_nivel2["detalle_faltantes"],
                "lista_sobrantes": list(resultado_nivel2["sobrantes"])
            },
            "detalle_hojas_destino": {
                "Resultados": len(placas_resultados),
                "Datos Runt": len(placas_datos_runt),
                "Datos Vehiculo": len(placas_datos_vehiculo),
                "Sin Asociados": len(placas_sin_asociados)
            }
        }
        
        archivo_json = LOGS_RECTIFICAR / f"reporte_completo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(archivo_json, 'w', encoding='utf-8') as f:
            json.dump(reporte_completo, f, indent=2, ensure_ascii=False)
        logger.info(f"\n💾 Reporte JSON guardado en: {archivo_json}")
        
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {e}", exc_info=True)
    
    logger.info("\n" + "="*80)
    logger.info("🏁 FIN DEL PROCESO DE RECTIFICACIÓN")
    logger.info("="*80)

if __name__ == "__main__":
    main()