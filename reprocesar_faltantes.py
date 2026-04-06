# reprocesar_faltantes.py
import logging
import sys
import time
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict

import gspread
from google.oauth2.service_account import Credentials

# Importar funciones de Runt.py
from Runt import (
    iniciar_driver, cerrar_driver, procesar_consulta, guardar_en_sheets,
    escribir_datos_vehiculo_en_sheets, guardar_resultado_en_resultados,
    limpiar_todos_los_campos, logging as runt_logging
)

# ═════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ═════════════════════════════════════════════════════════════

BASE_PATH = Path(r"C:\Users\cmarroquin\Music\RuntPro")
LOGS_REPROCESO = BASE_PATH / "reproceso_faltantes"
LOGS_REPROCESO.mkdir(parents=True, exist_ok=True)

GOOGLE_CREDS = BASE_PATH / "prueba-de-gmail-486215-345473339c47.json"

# IDs de los spreadsheets
DESTINO_SPREADSHEET_ID = "1vs414iH3QVeLoTcY2CExg4kD9eCkXZRRfax_WTlUXPk"
ORIGEN_SPREADSHEET_ID = "1saIDw37nd-rnzZvvKjxUQP41LhXJvSiayYgFRR78N7o"

# ═════════════════════════════════════════════════════════════
# CONFIGURACIÓN DE LOGS INDEPENDIENTES
# ═════════════════════════════════════════════════════════════

def configurar_logs():
    logger = logging.getLogger('reproceso')
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    fecha = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_handler = logging.FileHandler(
        str(LOGS_REPROCESO / f"reproceso_{fecha}.log"),
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

logger = configurar_logs()

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
# 1. IDENTIFICAR PLACAS FALTANTES (Funcionó en Resultados pero NO en Datos Runt)
# ═════════════════════════════════════════════════════════════

def identificar_placas_faltantes(client):
    """
    Compara Resultados vs Datos Runt
    Retorna lista de placas que dicen "Funcionó" en Resultados pero NO están en Datos Runt
    """
    
    logger.info("\n" + "="*80)
    logger.info("🔍 PASO 1: IDENTIFICANDO PLACAS FALTANTES")
    logger.info("="*80)
    
    try:
        sheet = client.open_by_key(DESTINO_SPREADSHEET_ID)
        
        # ═══ LEER RESULTADOS ═══
        logger.info("\n📄 Leyendo hoja 'Resultados'...")
        worksheet_resultados = sheet.worksheet("Resultados")
        todas_filas_resultados = worksheet_resultados.get_all_values()
        
        # Diccionario: placa -> {estado, cedula_asociado, cedula_propietario, cedula_usada}
        resultados_dict = {}
        
        for i in range(1, len(todas_filas_resultados)):
            fila = todas_filas_resultados[i]
            if len(fila) >= 6:
                placa = str(fila[2]).strip().upper()
                estado = str(fila[4]).strip()
                cedula_asociado = str(fila[0]).strip()
                cedula_propietario = str(fila[1]).strip()
                cedula_usada = str(fila[5]).strip()
                
                if placa and placa != "PLACA":
                    resultados_dict[placa] = {
                        "estado": estado,
                        "cedula_asociado": cedula_asociado,
                        "cedula_propietario": cedula_propietario,
                        "cedula_usada": cedula_usada,
                        "fila_resultados": i + 1
                    }
        
        logger.info(f"   Total en Resultados: {len(resultados_dict)}")
        
        # Contar estados
        funcionaron = sum(1 for v in resultados_dict.values() if "Funcionó" in v["estado"])
        fallaron = sum(1 for v in resultados_dict.values() if "Falló" in v["estado"])
        sin_personas = sum(1 for v in resultados_dict.values() if "Sin personas" in v["estado"])
        
        logger.info(f"   Desglose: Funcionó={funcionaron}, Falló={fallaron}, Sin personas={sin_personas}")
        
        # ═══ LEER DATOS RUNT ═══
        logger.info("\n📄 Leyendo hoja 'Datos Runt'...")
        worksheet_datos_runt = sheet.worksheet("Datos Runt")
        todas_filas_runt = worksheet_datos_runt.get_all_values()
        
        placas_en_datos_runt = set()
        for i in range(1, len(todas_filas_runt)):
            fila = todas_filas_runt[i]
            if len(fila) > 2:
                placa = str(fila[2]).strip().upper()
                if placa and placa != "PLACA":
                    placas_en_datos_runt.add(placa)
        
        logger.info(f"   Total en Datos Runt: {len(placas_en_datos_runt)}")
        
        # ═══ IDENTIFICAR FALTANTES ═══
        # Placas que dicen "Funcionó" en Resultados pero NO están en Datos Runt
        placas_faltantes = []
        
        for placa, info in resultados_dict.items():
            if "Funcionó" in info["estado"] and placa not in placas_en_datos_runt:
                placas_faltantes.append({
                    "placa": placa,
                    "cedula_asociado": info["cedula_asociado"],
                    "cedula_propietario": info["cedula_propietario"],
                    "cedula_usada": info["cedula_usada"],
                    "fila_resultados": info["fila_resultados"]
                })
        
        logger.info(f"\n📊 RESULTADO:")
        logger.info(f"   ✅ Placas que funcionaron y están en Datos Runt: {funcionaron - len(placas_faltantes)}")
        logger.info(f"   ❌ Placas que funcionaron pero NO están en Datos Runt: {len(placas_faltantes)}")
        
        if placas_faltantes:
            logger.info("\n   Lista de placas faltantes:")
            for item in placas_faltantes[:20]:
                logger.info(f"      {item['placa']} - Céd.Asoc: {item['cedula_asociado'][:12]}")
            if len(placas_faltantes) > 20:
                logger.info(f"      ... y {len(placas_faltantes) - 20} más")
        
        return placas_faltantes
        
    except Exception as e:
        logger.error(f"❌ Error identificando placas faltantes: {e}", exc_info=True)
        return []

# ═════════════════════════════════════════════════════════════
# 2. OBTENER DATOS DE ORIGEN (para tener cédula asociado y propietario)
# ═════════════════════════════════════════════════════════════

def obtener_datos_origen_para_placas(client, placas_faltantes):
    """
    Busca en las hojas de origen (Motos 0_5, etc.) las cédulas asociado y propietario
    para cada placa faltante
    """
    
    logger.info("\n" + "="*80)
    logger.info("🔍 PASO 2: OBTENIENDO DATOS DE ORIGEN")
    logger.info("="*80)
    
    if not placas_faltantes:
        return []
    
    try:
        sheet = client.open_by_key(ORIGEN_SPREADSHEET_ID)
        nombres_sheets = ["Motos 0_5", "Motos 6_10", "Motos 11_15", "Motos 16_25"]
        
        # Crear diccionario de búsqueda
        placas_dict = {item["placa"]: item for item in placas_faltantes}
        
        for nombre_sheet in nombres_sheets:
            try:
                worksheet = sheet.worksheet(nombre_sheet)
                cedulas_asociado = worksheet.col_values(2)   # Columna B
                cedulas_propietario = worksheet.col_values(4) # Columna D
                placas = worksheet.col_values(6)              # Columna F
                
                for i in range(1, min(len(cedulas_asociado), len(cedulas_propietario), len(placas))):
                    placa = str(placas[i]).strip().upper()
                    
                    if placa in placas_dict:
                        cedula_asoc = str(cedulas_asociado[i]).strip()
                        cedula_prop = str(cedulas_propietario[i]).strip()
                        
                        if cedula_asoc and cedula_asoc.lower() != "nan":
                            placas_dict[placa]["cedula_asociado_origen"] = cedula_asoc
                        if cedula_prop and cedula_prop.lower() != "nan":
                            placas_dict[placa]["cedula_propietario_origen"] = cedula_prop
                        
                        placas_dict[placa]["sheet_origen"] = nombre_sheet
                        placas_dict[placa]["fila_origen"] = i + 1
                        
            except Exception as e:
                logger.warning(f"   ⚠️ Error en sheet '{nombre_sheet}': {e}")
        
        # Validar que tenemos cédulas para todas
        datos_completos = []
        for item in placas_faltantes:
            if "cedula_asociado_origen" in item or "cedula_propietario_origen" in item:
                datos_completos.append(item)
            else:
                logger.warning(f"   ⚠️ No se encontraron cédulas para placa {item['placa']} en origen")
        
        logger.info(f"\n📊 RESULTADO:")
        logger.info(f"   ✅ Placas con datos completos: {len(datos_completos)}")
        logger.info(f"   ⚠️  Placas sin datos en origen: {len(placas_faltantes) - len(datos_completos)}")
        
        return datos_completos
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo datos de origen: {e}", exc_info=True)
        return []

# ═════════════════════════════════════════════════════════════
# 3. REPROCESAR PLACAS FALTANTES
# ═════════════════════════════════════════════════════════════

def reprocesar_placas(driver, placas_a_reprocesar):
    """
    Reprocesa cada placa usando las funciones de Runt.py
    """
    
    logger.info("\n" + "="*80)
    logger.info("🔄 PASO 3: REPROCESANDO PLACAS FALTANTES")
    logger.info("="*80)
    
    if not placas_a_reprocesar:
        logger.info("✅ No hay placas para reprocesar")
        return {"exitosos": [], "fallidos": []}
    
    resultados = {
        "exitosos": [],
        "fallidos": []
    }
    
    driver.get("https://portalpublico.runt.gov.co/#/consulta-vehiculo/consulta/consulta-ciudadana")
    time.sleep(4)
    limpiar_todos_los_campos(driver)
    
    for idx, item in enumerate(placas_a_reprocesar, 1):
        placa = item["placa"]
        cedula_asociado = item.get("cedula_asociado_origen", item.get("cedula_asociado", ""))
        cedula_propietario = item.get("cedula_propietario_origen", item.get("cedula_propietario", ""))
        
        logger.info(f"\n{'='*70}")
        logger.info(f"📊 Reprocesando [{idx}/{len(placas_a_reprocesar)}]: Placa {placa}")
        logger.info(f"   Cédula Asociado: {cedula_asociado}")
        logger.info(f"   Cédula Propietario: {cedula_propietario}")
        logger.info(f"{'='*70}")
        
        # Reiniciar sesión periódicamente cada 5 registros
        if idx > 1 and (idx - 1) % 5 == 0:
            logger.info(f"🔄 Reiniciando sesión después de {idx-1} procesados...")
            driver.get("https://www.google.com")
            time.sleep(2)
            driver.get("https://portalpublico.runt.gov.co/#/consulta-vehiculo/consulta/consulta-ciudadana")
            time.sleep(4)
            limpiar_todos_los_campos(driver)
        
        try:
            # Usar la función procesar_consulta de Runt.py
            resultado, _ = procesar_consulta(
                driver,
                cedula_asociado,
                cedula_propietario,
                placa,
                item.get("fila_origen", 0)
            )
            
            if resultado and resultado.get("estado") == "Exitoso":
                logger.info(f"✅ ¡ÉXITO! Placa {placa} reprocesada correctamente")
                
                # Guardar en sheets (actualizar existente)
                guardar_en_sheets([resultado], actualizar_existente=True)
                
                # Guardar datos del vehículo
                if "datos_vehiculo" in resultado and resultado["datos_vehiculo"]:
                    escribir_datos_vehiculo_en_sheets(resultado["datos_vehiculo"], cedula_asociado, placa)
                
                # Actualizar resultado en Resultados (ya existe, solo actualizar estado si cambió)
                guardar_resultado_en_resultados(
                    cedula_asociado,
                    cedula_propietario,
                    placa,
                    cedula_asociado,
                    "Exitoso"
                )
                
                resultados["exitosos"].append({
                    "placa": placa,
                    "cedula_usada": cedula_asociado
                })
                
            elif resultado and resultado.get("estado") == "Exitoso - Sin personas asociadas":
                logger.warning(f"⚠️ Sin personas asociadas para {placa}")
                resultados["fallidos"].append({
                    "placa": placa,
                    "razon": "Sin personas asociadas"
                })
            else:
                logger.error(f"❌ Falló reprocesamiento de {placa}")
                resultados["fallidos"].append({
                    "placa": placa,
                    "razon": "Error en consulta"
                })
                
        except Exception as e:
            logger.error(f"❌ Error reprocesando {placa}: {e}")
            resultados["fallidos"].append({
                "placa": placa,
                "razon": str(e)[:100]
            })
        
        # Pequeña pausa entre consultas
        time.sleep(2)
    
    return resultados

# ═════════════════════════════════════════════════════════════
# 4. GENERAR REPORTE FINAL
# ═════════════════════════════════════════════════════════════

def generar_reporte_final(placas_identificadas, resultados_reproceso):
    """
    Genera reporte detallado del proceso
    """
    
    logger.info("\n" + "="*80)
    logger.info("📊 REPORTE FINAL")
    logger.info("="*80)
    
    total_identificadas = len(placas_identificadas)
    exitosas = len(resultados_reproceso["exitosos"])
    fallidas = len(resultados_reproceso["fallidos"])
    
    logger.info(f"\n📌 RESUMEN:")
    logger.info(f"   🔍 Placas faltantes identificadas: {total_identificadas}")
    logger.info(f"   ✅ Reprocesadas exitosamente: {exitosas}")
    logger.info(f"   ❌ Fallaron en reproceso: {fallidas}")
    
    if total_identificadas > 0:
        porcentaje = (exitosas / total_identificadas) * 100
        logger.info(f"   📈 Tasa de éxito: {porcentaje:.1f}%")
    
    if resultados_reproceso["exitosos"]:
        logger.info(f"\n✅ PLACAS RECUPERADAS:")
        for item in resultados_reproceso["exitosos"]:
            logger.info(f"      {item['placa']} (Cédula: {item['cedula_usada']})")
    
    if resultados_reproceso["fallidos"]:
        logger.info(f"\n❌ PLACAS QUE AÚN FALLAN:")
        for item in resultados_reproceso["fallidos"]:
            logger.info(f"      {item['placa']} - Razón: {item['razon']}")
    
    # Guardar reporte en JSON
    reporte = {
        "fecha": datetime.now().isoformat(),
        "total_identificadas": total_identificadas,
        "exitosas": exitosas,
        "fallidas": fallidas,
        "detalle_exitosas": resultados_reproceso["exitosos"],
        "detalle_fallidas": resultados_reproceso["fallidos"],
        "lista_placas_identificadas": [p["placa"] for p in placas_identificadas]
    }
    
    archivo_reporte = LOGS_REPROCESO / f"reporte_reproceso_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(archivo_reporte, 'w', encoding='utf-8') as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)
    
    logger.info(f"\n💾 Reporte guardado en: {archivo_reporte}")
    
    # Guardar lista de placas que aún fallan (para posible reproceso manual)
    if resultados_reproceso["fallidos"]:
        archivo_fallidos = LOGS_REPROCESO / f"placas_a_revisar_manualmente_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(archivo_fallidos, 'w', encoding='utf-8') as f:
            f.write("PLACAS QUE AÚN FALLAN - REQUIEREN REVISIÓN MANUAL\n")
            f.write("="*60 + "\n\n")
            for item in resultados_reproceso["fallidos"]:
                f.write(f"{item['placa']} - {item['razon']}\n")
        logger.info(f"💾 Lista de fallidos guardada en: {archivo_fallidos}")

# ═════════════════════════════════════════════════════════════
# 5. FUNCIÓN PRINCIPAL
# ═════════════════════════════════════════════════════════════

def main():
    """Función principal - Reprocesa placas que funcionaron pero no están en Datos Runt"""
    
    logger.info("="*80)
    logger.info("🚀 INICIANDO REPROCESO DE PLACAS FALTANTES")
    logger.info("="*80)
    logger.info(f"📁 Logs guardados en: {LOGS_REPROCESO}")
    logger.info(f"🕐 Fecha/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    driver = None
    
    try:
        # Conectar a Google Sheets
        client = conectar_google_sheets()
        logger.info("✅ Conexión a Google Sheets establecida")
        
        # PASO 1: Identificar placas faltantes
        placas_faltantes = identificar_placas_faltantes(client)
        
        if not placas_faltantes:
            logger.info("\n✅ No se encontraron placas faltantes. Todo está consistente.")
            return
        
        # PASO 2: Obtener datos de origen
        placas_con_datos = obtener_datos_origen_para_placas(client, placas_faltantes)
        
        if not placas_con_datos:
            logger.error("❌ No se pudieron obtener datos de origen para ninguna placa")
            return
        
        # PASO 3: Iniciar driver y reprocesar
        logger.info("\n🚗 Iniciando navegador para reprocesamiento...")
        driver = iniciar_driver()
        
        if not driver:
            logger.error("❌ No se pudo iniciar el driver")
            return
        
        driver.maximize_window()
        
        # Reprocesar placas
        resultados = reprocesar_placas(driver, placas_con_datos)
        
        # PASO 4: Generar reporte final
        generar_reporte_final(placas_con_datos, resultados)
        
        logger.info("\n" + "="*80)
        logger.info("✅ PROCESO DE REPROCESO COMPLETADO")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {e}", exc_info=True)
    finally:
        if driver:
            cerrar_driver(driver)

if __name__ == "__main__":
    main()