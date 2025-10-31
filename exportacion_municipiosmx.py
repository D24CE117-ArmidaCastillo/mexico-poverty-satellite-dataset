"""
EXPORTACIÓN ROBUSTA DE IMÁGENES SATELITALES Y ESTADÍSTICAS POR MUNICIPIO - MÉXICO

REQUISITOS:
pip install earthengine-api tqdm tenacity

EJECUCIÓN:
python exportacion_municipios_mx.py --estado AGS --dry-run  # Validar
python exportacion_municipios_mx.py --estado AGS            # Ejecutar
python exportacion_municipios_mx.py --all --max-concurrent 150  # Producción
"""

import ee
import time
import json
import os
import sys
import argparse
import logging
import tempfile
import traceback
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict, field
from enum import Enum
from collections import defaultdict

# Dependencias externas
try:
    from tqdm import tqdm
    from tenacity import (
        retry, 
        stop_after_attempt,
        stop_after_delay,
        wait_exponential,
        wait_random_exponential,
        retry_if_exception_type,
        before_sleep_log
    )
except ImportError:
    print("ERROR: Instalar dependencias requeridas:")
    print("pip install tqdm tenacity")
    sys.exit(1)

# ======================================================================
# CONFIGURACIÓN
# ======================================================================

PROJECT_ID = 'poverty-prediction-457518'
ASSET_MUNICIPIOS = 'projects/poverty-prediction-457518/assets/Mexico'

# Control de concurrencia
MAX_CONCURRENT_TASKS = 200
TASK_CHECK_INTERVAL = 30
MAX_RETRY_ATTEMPTS = 5
MAX_RETRY_TIMEOUT = 600

# Estructura de carpetas Drive (ACTUALIZADA)
DRIVE_ROOT = "M-Dataset"
DRIVE_COUNTRY = "M-Mexico"
# Nivel 3: [NombreEstado]
# Nivel 4: M-[CodMun]

# Archivos de caché
CACHE_DIR = Path('cache')
CACHE_DIR.mkdir(exist_ok=True)
CACHE_FILE = CACHE_DIR / 'tareas_completadas_municipios.json'
CACHE_TASKS_FILE = CACHE_DIR / 'tareas_activas_municipios.json'
METRICS_FILE = CACHE_DIR / 'metricas_municipios.json'
LOG_FILE = CACHE_DIR / f'exportacion_municipios_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'

# Periodo de análisis
FECHA_INICIO = '2020-01-01'
FECHA_FIN = '2020-12-31'

# Estados de México
ESTADOS_INFO = {
    '01': {'nombre': 'Aguascalientes', 'codigo': 'AGS'},
    '02': {'nombre': 'Baja California', 'codigo': 'BC'},
    '03': {'nombre': 'Baja California Sur', 'codigo': 'BCS'},
    '04': {'nombre': 'Campeche', 'codigo': 'CAMP'},
    '05': {'nombre': 'Coahuila de Zaragoza', 'codigo': 'COAH'},
    '06': {'nombre': 'Colima', 'codigo': 'COL'},
    '07': {'nombre': 'Chiapas', 'codigo': 'CHIS'},
    '08': {'nombre': 'Chihuahua', 'codigo': 'CHIH'},
    '09': {'nombre': 'Ciudad de Mexico', 'codigo': 'CDMX'},
    '10': {'nombre': 'Durango', 'codigo': 'DGO'},
    '11': {'nombre': 'Guanajuato', 'codigo': 'GTO'},
    '12': {'nombre': 'Guerrero', 'codigo': 'GRO'},
    '13': {'nombre': 'Hidalgo', 'codigo': 'HGO'},
    '14': {'nombre': 'Jalisco', 'codigo': 'JAL'},
    '15': {'nombre': 'Mexico', 'codigo': 'MEX'},
    '16': {'nombre': 'Michoacan de Ocampo', 'codigo': 'MICH'},
    '17': {'nombre': 'Morelos', 'codigo': 'MOR'},
    '18': {'nombre': 'Nayarit', 'codigo': 'NAY'},
    '19': {'nombre': 'Nuevo Leon', 'codigo': 'NL'},
    '20': {'nombre': 'Oaxaca', 'codigo': 'OAX'},
    '21': {'nombre': 'Puebla', 'codigo': 'PUE'},
    '22': {'nombre': 'Queretaro', 'codigo': 'QRO'},
    '23': {'nombre': 'Quintana Roo', 'codigo': 'QROO'},
    '24': {'nombre': 'San Luis Potosi', 'codigo': 'SLP'},
    '25': {'nombre': 'Sinaloa', 'codigo': 'SIN'},
    '26': {'nombre': 'Sonora', 'codigo': 'SON'},
    '27': {'nombre': 'Tabasco', 'codigo': 'TAB'},
    '28': {'nombre': 'Tamaulipas', 'codigo': 'TAMPS'},
    '29': {'nombre': 'Tlaxcala', 'codigo': 'TLAX'},
    '30': {'nombre': 'Veracruz de Ignacio de la Llave', 'codigo': 'VER'},
    '31': {'nombre': 'Yucatan', 'codigo': 'YUC'},
    '32': {'nombre': 'Zacatecas', 'codigo': 'ZAC'}
}

# ======================================================================
# LOGGING SETUP
# ======================================================================

def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configura logging dual: archivo + consola"""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    # Handler de archivo
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Handler de consola
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_formatter = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

# ======================================================================
# CLASES DE DATOS
# ======================================================================

@dataclass
class ExecutionMetrics:
    """Métricas de ejecución"""
    tareas_enviadas: int = 0
    tareas_completadas: int = 0
    tareas_fallidas: int = 0
    tareas_omitidas: int = 0
    tiempo_total: float = 0.0
    tiempo_promedio_tarea: float = 0.0
    errores_por_tipo: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    
    def tasa_exito(self) -> float:
        total = self.tareas_enviadas + self.tareas_omitidas
        if total == 0:
            return 0.0
        return (self.tareas_completadas + self.tareas_omitidas) / total * 100

# ======================================================================
# SISTEMA DE CACHÉ ATÓMICO
# ======================================================================

class AtomicCacheManager:
    """Gestiona caché con escrituras atómicas"""
    
    def __init__(self, cache_file: Path = CACHE_FILE, tasks_file: Path = CACHE_TASKS_FILE):
        self.cache_file = cache_file
        self.tasks_file = tasks_file
        self.completed = self._load_json(cache_file)
        self.active_tasks = self._load_json(tasks_file)
        logger.info(f"Caché cargado: {len(self.completed)} completadas, {len(self.active_tasks)} activas")
    
    def _load_json(self, filepath: Path) -> Dict:
        try:
            if filepath.exists():
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not isinstance(data, dict):
                        logger.warning(f"Caché {filepath} no es dict, creando nuevo")
                        return {}
                    return data
            logger.debug(f"Caché {filepath} no existe, inicializando vacío")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Caché {filepath} corrupto: {e}. Creando backup")
            backup = filepath.with_suffix('.json.bak')
            if filepath.exists():
                filepath.rename(backup)
            return {}
        except Exception as e:
            logger.error(f"Error cargando {filepath}: {e}")
            return {}
    
    def _save_json_atomic(self, data: Dict, filepath: Path):
        try:
            tmp_fd, tmp_path = tempfile.mkstemp(
                dir=filepath.parent, 
                prefix=f'.{filepath.stem}_', 
                suffix='.tmp'
            )
            
            with os.fdopen(tmp_fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            
            os.replace(tmp_path, filepath)
            logger.debug(f"Caché guardado: {filepath}")
            
        except Exception as e:
            logger.error(f"Error guardando {filepath}: {e}")
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                try:
                    os.unlink(tmp_path)
                except:
                    pass
    
    def is_completed(self, task_id: str) -> bool:
        return task_id in self.completed
    
    def is_active(self, task_id: str) -> bool:
        return task_id in self.active_tasks
    
    def mark_completed(self, task_id: str, info: Optional[Dict] = None):
        self.completed[task_id] = {
            'timestamp': datetime.now().isoformat(),
            'info': info or {}
        }
        self._save_json_atomic(self.completed, self.cache_file)
        
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]
            self._save_json_atomic(self.active_tasks, self.tasks_file)
            logger.debug(f"Tarea movida a completadas: {task_id}")
    
    def mark_active(self, task_id: str, gee_task_id: str, info: Optional[Dict] = None):
        self.active_tasks[task_id] = {
            'gee_id': gee_task_id,
            'timestamp': datetime.now().isoformat(),
            'info': info or {}
        }
        self._save_json_atomic(self.active_tasks, self.tasks_file)
        logger.debug(f"Tarea marcada activa: {task_id} -> {gee_task_id}")
    
    def mark_failed(self, task_id: str, error: str):
        if task_id in self.active_tasks:
            del self.active_tasks[task_id]
            self._save_json_atomic(self.active_tasks, self.tasks_file)
            logger.warning(f"Tarea marcada como fallida: {task_id} - {error}")
    
    @retry(
        stop=stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_RETRY_TIMEOUT),
        wait=wait_random_exponential(multiplier=2, min=4, max=120),
        retry=retry_if_exception_type((ee.EEException, ConnectionError)),
        before_sleep=before_sleep_log(logger, logging.WARNING)
    )
    def check_active_tasks(self) -> Tuple[int, int, int]:
        """Verifica estado de tareas activas en GEE"""
        try:
            logger.info(f"Verificando {len(self.active_tasks)} tareas activas en GEE...")
            
            gee_tasks = ee.data.getTaskList()
            active_states = {'RUNNING', 'PENDING', 'READY', 'COMPLETED', 'FAILED', 'CANCELLED'}
            gee_tasks_dict = {
                t['id']: t for t in gee_tasks 
                if t.get('state') in active_states
            }
            
            logger.debug(f"Tareas GEE relevantes: {len(gee_tasks_dict)}")
            
            completed_count = 0
            failed_count = 0
            to_remove = []
            
            for task_id, info in list(self.active_tasks.items()):
                gee_id = info.get('gee_id')
                
                if not gee_id:
                    logger.warning(f"Tarea {task_id} sin gee_id, removiendo")
                    to_remove.append(task_id)
                    continue
                
                if gee_id in gee_tasks_dict:
                    state = gee_tasks_dict[gee_id]['state']
                    
                    if state == 'COMPLETED':
                        self.mark_completed(task_id, info.get('info'))
                        completed_count += 1
                        logger.debug(f"✓ Tarea completada: {task_id}")
                    
                    elif state in ['FAILED', 'CANCELLED']:
                        error_msg = gee_tasks_dict[gee_id].get('error_message', 'Unknown error')
                        logger.error(f"✗ Tarea fallida: {task_id} - {error_msg}")
                        self.mark_failed(task_id, error_msg)
                        failed_count += 1
                
                else:
                    timestamp_str = info.get('timestamp', '')
                    if timestamp_str:
                        try:
                            task_time = datetime.fromisoformat(timestamp_str)
                            if datetime.now() - task_time > timedelta(days=7):
                                logger.warning(f"Tarea {task_id} muy antigua (>7 días), removiendo")
                                to_remove.append(task_id)
                        except:
                            pass
            
            for task_id in to_remove:
                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]
            
            if to_remove:
                self._save_json_atomic(self.active_tasks, self.tasks_file)
            
            active_remaining = len(self.active_tasks)
            logger.info(f"Estado: {completed_count} completadas, {failed_count} fallidas, {active_remaining} activas")
            
            return completed_count, failed_count, active_remaining
        
        except ee.EEException as e:
            logger.error(f"Error comunicación GEE: {e}")
            raise
        except Exception as e:
            logger.error(f"Error verificando tareas: {e}")
            return 0, 0, len(self.active_tasks)

# ======================================================================
# CONTROL DE CONCURRENCIA
# ======================================================================

class TaskThrottler:
    """Controla número de tareas concurrentes en GEE"""
    
    def __init__(self, max_concurrent: int, check_interval: int, cache_manager: AtomicCacheManager):
        self.max_concurrent = max_concurrent
        self.check_interval = check_interval
        self.cache = cache_manager
        self.last_check = datetime.now()
    
    def wait_for_capacity(self, dry_run: bool = False):
        if dry_run:
            return
        
        while True:
            active_count = len(self.cache.active_tasks)
            
            if active_count < self.max_concurrent:
                logger.debug(f"Capacidad disponible: {active_count}/{self.max_concurrent}")
                return
            
            if (datetime.now() - self.last_check).seconds >= self.check_interval:
                logger.info(f"Límite alcanzado ({active_count}/{self.max_concurrent}), verificando...")
                self.cache.check_active_tasks()
                self.last_check = datetime.now()
            
            logger.info(f"Esperando capacidad... ({active_count}/{self.max_concurrent})")
            time.sleep(self.check_interval)

# ======================================================================
# CARGA DE DATOS SATELITALES
# ======================================================================

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_RETRY_TIMEOUT),
    wait=wait_random_exponential(multiplier=2, min=4, max=120),
    retry=retry_if_exception_type(ee.EEException),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def cargar_datos_satelitales() -> Optional[Dict]:
    """Carga colecciones satelitales con reintentos"""
    logger.info("Cargando colecciones satelitales...")
    
    try:
        # Sentinel-2 NDVI
        s2 = ee.ImageCollection('COPERNICUS/S2') \
            .filterDate(FECHA_INICIO, FECHA_FIN) \
            .filter(ee.Filter.lt('CLOUDY_PIXEL_PERCENTAGE', 10)) \
            .select(["B4", "B8"]) \
            .median()
        ndvi = s2.normalizedDifference(['B8', 'B4']).rename('NDVI')
        
        # ESA WorldCover 2020
        landcover = ee.Image('ESA/WorldCover/v100/2020').select('Map').rename('LandCover')
        
        # VIIRS Night Lights
        viirs = ee.ImageCollection('NOAA/VIIRS/DNB/MONTHLY_V1/VCMSLCFG') \
            .filterDate(FECHA_INICIO, FECHA_FIN) \
            .select('avg_rad') \
            .mean() \
            .rename('NightLights')
        
        # Water Occurrence
        water = ee.Image('JRC/GSW1_4/GlobalSurfaceWater') \
            .select('occurrence').gte(90).selfMask().rename('Water')
        
        # Temperatura (Celsius)
        era5_temp = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY') \
            .filterDate(FECHA_INICIO, FECHA_FIN) \
            .select('temperature_2m') \
            .mean() \
            .subtract(273.15) \
            .rename('Temperature')
        
        # Precipitación (mm)
        era5_precip = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY') \
            .filterDate(FECHA_INICIO, FECHA_FIN) \
            .select('total_precipitation_hourly') \
            .sum() \
            .multiply(1000) \
            .rename('Precipitation')
        
        logger.info("✓ Colecciones satelitales cargadas correctamente")
        
        return {
            'ndvi': ndvi,
            'landcover': landcover,
            'viirs': viirs,
            'water': water,
            'temperature': era5_temp,
            'precipitation': era5_precip
        }
        
    except ee.EEException as e:
        logger.error(f"Error GEE cargando colecciones: {e}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado cargando colecciones: {e}")
        return None

# ======================================================================
# EXPORTACIÓN DE ESTADÍSTICAS
# ======================================================================

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_RETRY_TIMEOUT),
    wait=wait_random_exponential(multiplier=2, min=4, max=120),
    retry=retry_if_exception_type(ee.EEException),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def calcular_estadisticas_estado(
    fc_estado: ee.FeatureCollection,
    codigo_estado: str,
    nombre_estado: str,
    datos_satelitales: Dict,
    cache: AtomicCacheManager,
    throttler: TaskThrottler,
    dry_run: bool = False
) -> Dict:
    """Calcula estadísticas zonales para todos los municipios del estado"""
    task_id = f"STATS_{codigo_estado}_2020"
    
    if cache.is_completed(task_id):
        logger.info(f"  ⏭ Estadísticas {codigo_estado} ya completadas")
        return {'success': True, 'skipped': True}
    
    if cache.is_active(task_id):
        logger.info(f"  ⏳ Estadísticas {codigo_estado} en proceso")
        return {'success': True, 'skipped': True}
    
    try:
        logger.info(f"  📊 Calculando estadísticas para {nombre_estado}...")
        
        def zonal_stats(image: ee.Image, nombre_variable: str, escala: int):
            try:
                stats = image.reduceRegions(
                    collection=fc_estado,
                    reducer=ee.Reducer.mean().combine(
                        reducer2=ee.Reducer.stdDev(),
                        sharedInputs=True
                    ).combine(
                        reducer2=ee.Reducer.minMax(),
                        sharedInputs=True
                    ),
                    scale=escala,
                )
                return stats.map(lambda f: f.set('variable', nombre_variable))
            except Exception as e:
                logger.warning(f"    ⚠ Error calculando {nombre_variable}: {e}")
                return None
        
        all_stats_list = []
        
        stats_configs = [
            (datos_satelitales['ndvi'], 'NDVI', 10),
            (datos_satelitales['landcover'], 'LandCover', 10),
            (datos_satelitales['viirs'], 'NightLights', 500),
            (datos_satelitales['water'], 'Water', 30),
            (datos_satelitales['temperature'], 'Temperature', 11000),
            (datos_satelitales['precipitation'], 'Precipitation', 11000)
        ]
        
        for img, name, scale in stats_configs:
            result = zonal_stats(img, name, scale)
            if result is not None:
                all_stats_list.append(result)
        
        if not all_stats_list:
            logger.error(f"  ✗ No se pudieron calcular estadísticas para {codigo_estado}")
            return {'success': False, 'error': 'No statistics calculated'}
        
        all_stats = ee.FeatureCollection(all_stats_list).flatten()
        
        folder_name = nombre_estado
        description = f"{codigo_estado}_Estadisticas_2020"
        
        if dry_run:
            logger.info(f"  [DRY-RUN] CSV: {description}.csv → {folder_name}/")
            return {'success': True, 'skipped': False, 'dry_run': True}
        
        throttler.wait_for_capacity()
        
        task = ee.batch.Export.table.toDrive(
            collection=all_stats,
            description=description,
            folder=folder_name,
            fileFormat='CSV',
            selectors=['CVE_ENT', 'CVE_MUN', 'NOMGEO', 
                      'variable', 'mean', 'stdDev', 'min', 'max']
        )
        
        task.start()
        
        cache.mark_active(task_id, task.id, {
            'estado': codigo_estado,
            'tipo': 'estadisticas',
            'descripcion': description,
            'folder': folder_name
        })
        
        logger.info(f"  ✓ Estadísticas enviadas: {description} [Task ID: {task.id}]")
        return {'success': True, 'skipped': False, 'task_id': task.id}
        
    except ee.EEException as e:
        logger.error(f"  ✗ Error GEE en estadísticas {codigo_estado}: {e}")
        raise
    except Exception as e:
        logger.error(f"  ✗ Error inesperado en estadísticas: {e}")
        return {'success': False, 'error': str(e)}

# ======================================================================
# EXPORTACIÓN DE IMÁGENES
# ======================================================================

@retry(
    stop=stop_after_attempt(MAX_RETRY_ATTEMPTS) | stop_after_delay(MAX_RETRY_TIMEOUT),
    wait=wait_random_exponential(multiplier=2, min=5, max=180),
    retry=retry_if_exception_type(ee.EEException),
    before_sleep=before_sleep_log(logger, logging.WARNING)
)
def exportar_imagen_unica(
    image: ee.Image,
    description: str,
    folder: str,
    region: ee.Geometry,
    scale: int,
    task_id: str,
    cache: AtomicCacheManager,
    throttler: TaskThrottler,
    dry_run: bool = False
) -> Dict:
    """Exporta una imagen con verificación de duplicados y reintentos"""
    if cache.is_completed(task_id):
        return {'success': True, 'skipped': True}
    
    if cache.is_active(task_id):
        return {'success': True, 'skipped': True}
    
    try:
        if dry_run:
            logger.debug(f"    [DRY-RUN] {description}.tif → {folder}/")
            return {'success': True, 'skipped': False, 'dry_run': True}
        
        throttler.wait_for_capacity()
        
        task = ee.batch.Export.image.toDrive(
            image=image,
            description=description,
            folder=folder,
            region=region,
            scale=scale,
            maxPixels=1e13,
            fileFormat='GeoTIFF'
        )
        
        task.start()
        
        cache.mark_active(task_id, task.id, {
            'descripcion': description,
            'folder': folder,
            'tipo': 'imagen'
        })
        
        logger.debug(f"    ✓ Imagen enviada: {description} [Task ID: {task.id}]")
        return {'success': True, 'skipped': False, 'task_id': task.id}
        
    except ee.EEException as e:
        error_str = str(e)
        
        if 'Quota exceeded' in error_str or 'Too many concurrent' in error_str:
            logger.warning(f"    ⚠ Cuota GEE excedida, aplicando backoff...")
            time.sleep(120)
            raise
        
        if 'folder' in error_str.lower() or 'not found' in error_str.lower():
            logger.error(f"    ✗ ERROR: Carpeta no existe en Drive: {folder}")
            return {'success': False, 'error': f'Folder not found: {folder}'}
        
        logger.error(f"    ✗ Error GEE exportando {description}: {e}")
        raise
        
    except Exception as e:
        logger.error(f"    ✗ Error inesperado exportando {description}: {e}")
        return {'success': False, 'error': str(e)}

# ======================================================================
# PROCESAMIENTO POR ESTADO
# ======================================================================

def exportar_estado(
    estado_id: str,
    codigo_estado: str,
    nombre_estado: str,
    datos_satelitales: Dict,
    cache: AtomicCacheManager,
    throttler: TaskThrottler,
    metrics: ExecutionMetrics,
    dry_run: bool = False
) -> Dict:
    """Exporta imágenes y estadísticas para todos los municipios del estado"""
    logger.info(f"\n{'='*70}")
    logger.info(f"ESTADO: {nombre_estado} ({codigo_estado})")
    logger.info(f"{'='*70}")
    
    inicio_estado = time.time()
    
    try:
        # Cargar municipios del estado
        fc_estado = ee.FeatureCollection(ASSET_MUNICIPIOS) \
            .filter(ee.Filter.eq('CVE_ENT', estado_id))
        
        logger.info("Obteniendo metadatos de municipios...")
        
        total_municipios = fc_estado.size().getInfo()
        municipios_info = fc_estado.select(['CVE_MUN', 'NOMGEO']).getInfo()['features']
        
        logger.info(f"Total de municipios: {total_municipios}")
        
        if total_municipios == 0:
            logger.warning("⚠ No hay municipios")
            return {'tareas': 0, 'estadisticas': 0, 'errores': 0, 'omitidas': 0}
        
    except ee.EEException as e:
        logger.error(f"✗ Error GEE cargando estado: {e}")
        metrics.errores_por_tipo['gee_connection'] += 1
        return {'tareas': 0, 'estadisticas': 0, 'errores': 1, 'omitidas': 0}
    except Exception as e:
        logger.error(f"✗ Error cargando estado: {e}")
        metrics.errores_por_tipo['unknown'] += 1
        return {'tareas': 0, 'estadisticas': 0, 'errores': 1, 'omitidas': 0}
    
    # EXPORTAR ESTADÍSTICAS DEL ESTADO
    logger.info(f"\n📊 Estadísticas del estado:")
    stats_result = calcular_estadisticas_estado(
        fc_estado, codigo_estado, nombre_estado, 
        datos_satelitales, cache, throttler, dry_run
    )
    
    total_tareas = 0
    total_estadisticas = 1 if stats_result.get('success') and not stats_result.get('skipped') else 0
    total_errores = 0 if stats_result.get('success') else 1
    total_omitidas = 0
    
    if not stats_result.get('success'):
        metrics.errores_por_tipo['estadisticas'] += 1
    
    # Procesar cada municipio con barra de progreso
    logger.info(f"\nProcesando {len(municipios_info)} municipios...")
    
    for idx, feature in enumerate(tqdm(municipios_info, desc=f"{codigo_estado}", unit="mun"), 1):
        try:
            props = feature['properties']
            cve_mun = props['CVE_MUN']
            nom_mun = props.get('NOMGEO', 'UNKNOWN')
            
            # Obtener geometría del municipio (server-side)
            fc_mun = fc_estado.filter(ee.Filter.eq('CVE_MUN', cve_mun))
            geom = fc_mun.geometry()
            
            # Carpeta del municipio con prefijo M-
            folder_municipio = f"M-{codigo_estado}{cve_mun}"
            prefix = f"{codigo_estado}{cve_mun}"
            
            logger.debug(f"  [{idx}/{len(municipios_info)}] Procesando: {prefix} ({nom_mun})")
            
            # Configuración de imágenes
            configs = [
                {'key': 'ndvi', 'name': 'NDVI', 'scale': 10, 'type': 'float'},
                {'key': 'landcover', 'name': 'LandCover', 'scale': 10, 'type': 'uint8'},
                {'key': 'viirs', 'name': 'NightLights', 'scale': 500, 'type': 'float'},
                {'key': 'water', 'name': 'Water', 'scale': 30, 'type': 'uint8'},
                {'key': 'temperature', 'name': 'Temperature', 'scale': 11000, 'type': 'float'},
                {'key': 'precipitation', 'name': 'Precipitation', 'scale': 11000, 'type': 'float'}
            ]
            
            errores_mun = 0
            omitidas_mun = 0
            nuevas_mun = 0
            
            # Exportar cada imagen
            for cfg in configs:
                task_id = f"IMG_{prefix}_{cfg['name']}_2020"
                
                img = datos_satelitales[cfg['key']]
                img_clip = img.clip(geom)
                
                # Convertir tipo según configuración
                if cfg['type'] == 'float':
                    img_export = img_clip.toFloat()
                else:
                    img_export = img_clip.toUint8()
                
                inicio_tarea = time.time()
                
                result = exportar_imagen_unica(
                    img_export,
                    f"{prefix}_{cfg['name']}_2020",
                    folder_municipio,
                    geom.bounds(),
                    cfg['scale'],
                    task_id,
                    cache,
                    throttler,
                    dry_run
                )
                
                tiempo_tarea = time.time() - inicio_tarea
                
                if result['success']:
                    if result['skipped']:
                        omitidas_mun += 1
                        metrics.tareas_omitidas += 1
                    else:
                        nuevas_mun += 1
                        total_tareas += 1
                        metrics.tareas_enviadas += 1
                        
                        # Actualizar métricas de tiempo
                        if metrics.tiempo_promedio_tarea == 0:
                            metrics.tiempo_promedio_tarea = tiempo_tarea
                        else:
                            metrics.tiempo_promedio_tarea = (
                                metrics.tiempo_promedio_tarea * 0.9 + tiempo_tarea * 0.1
                            )
                else:
                    errores_mun += 1
                    error_msg = result.get('error', 'Unknown')
                    logger.error(f"    ✗ Error en {prefix}_{cfg['name']}: {error_msg}")
                    metrics.errores_por_tipo['export_failed'] += 1
            
            # Resumen del municipio
            if nuevas_mun > 0 or omitidas_mun > 0 or errores_mun > 0:
                logger.info(
                    f"  Municipio {cve_mun}: "
                    f"✓ {nuevas_mun} nuevas | "
                    f"⏭ {omitidas_mun} omitidas | "
                    f"✗ {errores_mun} errores"
                )
            
            total_omitidas += omitidas_mun
            total_errores += errores_mun
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logger.error(f"✗ Error en municipio {cve_mun}: {e}")
            total_errores += 1
            metrics.errores_por_tipo['municipio_error'] += 1
            continue
    
    # Tiempo total del estado
    tiempo_estado = time.time() - inicio_estado
    logger.info(f"\n⏱ Tiempo procesamiento estado: {tiempo_estado:.1f}s")
    
    return {
        'tareas': total_tareas,
        'estadisticas': total_estadisticas,
        'errores': total_errores,
        'omitidas': total_omitidas
    }

# ======================================================================
# GUARDADO DE MÉTRICAS
# ======================================================================

def guardar_metricas(metrics: ExecutionMetrics, filepath: Path = METRICS_FILE):
    """Guarda métricas de ejecución en JSON"""
    try:
        data = {
            'timestamp': datetime.now().isoformat(),
            'tareas_enviadas': metrics.tareas_enviadas,
            'tareas_completadas': metrics.tareas_completadas,
            'tareas_fallidas': metrics.tareas_fallidas,
            'tareas_omitidas': metrics.tareas_omitidas,
            'tiempo_total_segundos': metrics.tiempo_total,
            'tiempo_promedio_tarea_segundos': metrics.tiempo_promedio_tarea,
            'tasa_exito_porcentaje': metrics.tasa_exito(),
            'errores_por_tipo': dict(metrics.errores_por_tipo)
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Métricas guardadas en {filepath}")
        
    except Exception as e:
        logger.error(f"Error guardando métricas: {e}")

# ======================================================================
# FUNCIÓN PRINCIPAL
# ======================================================================

def main(args):
    """Función principal de ejecución"""
    
    inicio = datetime.now()
    metrics = ExecutionMetrics()
    
    logger.info("\n" + "="*70)
    logger.info("EXPORTACIÓN DE IMÁGENES Y ESTADÍSTICAS POR MUNICIPIO - MÉXICO")
    logger.info(f"Hora inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Modo: {'DRY-RUN (sin ejecutar tareas)' if args.dry_run else 'EJECUCIÓN REAL'}")
    logger.info(f"Concurrencia máxima: {args.max_concurrent}")
    logger.info(f"Log: {LOG_FILE}")
    logger.info("="*70)
    
    # Inicializar Earth Engine
    try:
        logger.info(f"Inicializando Earth Engine (proyecto: {PROJECT_ID})...")
        ee.Initialize(project=PROJECT_ID)
        logger.info("✓ Earth Engine inicializado correctamente")
    except Exception as e:
        logger.error(f"✗ Error inicializando GEE: {e}")
        logger.error("Ejecuta: earthengine authenticate")
        return 1
    
    # Inicializar caché y throttler
    cache = AtomicCacheManager()
    throttler = TaskThrottler(args.max_concurrent, TASK_CHECK_INTERVAL, cache)
    
    # Verificar tareas activas al inicio
    if not args.dry_run:
        logger.info("\n🔍 Verificando tareas activas en GEE...")
        try:
            completed, failed, active = cache.check_active_tasks()
            logger.info(f"   Estado inicial: {completed} completadas, {failed} fallidas, {active} activas")
            metrics.tareas_completadas = completed
            metrics.tareas_fallidas = failed
        except Exception as e:
            logger.warning(f"   ⚠ No se pudo verificar tareas: {e}")
    
    # Cargar datos satelitales
    datos_satelitales = cargar_datos_satelitales()
    if datos_satelitales is None:
        logger.error("✗ No se pudieron cargar datos satelitales")
        return 1
    
    # Determinar estados a procesar
    if args.all:
        estados_a_procesar = sorted(ESTADOS_INFO.keys())
        logger.info(f"\nProcesando TODOS los estados ({len(estados_a_procesar)})")
    elif args.estado:
        estado_id = None
        for eid, info in ESTADOS_INFO.items():
            if info['codigo'].upper() == args.estado.upper():
                estado_id = eid
                break
        
        if estado_id is None:
            logger.error(f"✗ Estado no encontrado: {args.estado}")
            logger.info(f"Estados disponibles: {', '.join([info['codigo'] for info in ESTADOS_INFO.values()])}")
            return 1
        
        estados_a_procesar = [estado_id]
        logger.info(f"\nProcesando estado: {ESTADOS_INFO[estado_id]['nombre']}")
    else:
        logger.error("✗ Debes especificar --estado CODIGO o --all")
        return 1
    
    # Procesar estados
    total_general = 0
    total_estadisticas = 0
    total_errores = 0
    total_omitidas = 0
    estados_ok = 0
    estados_error = []
    
    try:
        for estado_id in estados_a_procesar:
            info = ESTADOS_INFO[estado_id]
            
            try:
                resultado = exportar_estado(
                    estado_id, 
                    info['codigo'], 
                    info['nombre'],
                    datos_satelitales,
                    cache,
                    throttler,
                    metrics,
                    args.dry_run
                )
                
                total_general += resultado['tareas']
                total_estadisticas += resultado['estadisticas']
                total_errores += resultado['errores']
                total_omitidas += resultado['omitidas']
                
                if resultado['tareas'] > 0 or resultado['estadisticas'] > 0:
                    estados_ok += 1
                
                # Verificar tareas periódicamente
                if not args.dry_run and (estados_a_procesar.index(estado_id) + 1) % 5 == 0:
                    logger.info("\n🔍 Verificación periódica de tareas...")
                    try:
                        completed, failed, active = cache.check_active_tasks()
                        metrics.tareas_completadas = completed
                        metrics.tareas_fallidas = failed
                    except Exception as e:
                        logger.warning(f"⚠ Error en verificación: {e}")
                
            except KeyboardInterrupt:
                logger.warning("\n\n⚠ DETENIDO POR USUARIO (Ctrl+C)")
                break
                
            except Exception as e:
                logger.error(f"✗ Error crítico en {info['nombre']}: {e}")
                estados_error.append(info['nombre'])
                total_errores += 1
                metrics.errores_por_tipo['estado_error'] += 1
                continue
    
    finally:
        # Resumen final
        fin = datetime.now()
        duracion = fin - inicio
        metrics.tiempo_total = duracion.total_seconds()
        
        # Guardar métricas
        guardar_metricas(metrics)
        
        logger.info("\n" + "="*70)
        logger.info("RESUMEN FINAL")
        logger.info("="*70)
        logger.info(f"Inicio:                {inicio.strftime('%H:%M:%S')}")
        logger.info(f"Fin:                   {fin.strftime('%H:%M:%S')}")
        logger.info(f"Duración:              {duracion}")
        logger.info(f"\nEstados procesados:    {estados_ok}/{len(estados_a_procesar)}")
        logger.info(f"Imágenes nuevas:       {total_general:,}")
        logger.info(f"Imágenes omitidas:     {total_omitidas:,}")
        logger.info(f"CSVs de estadísticas:  {total_estadisticas:,}")
        logger.info(f"Errores encontrados:   {total_errores:,}")
        
        if estados_error:
            logger.warning(f"\nEstados con error:     {', '.join(estados_error)}")
        
        # Métricas adicionales
        logger.info(f"\n{'='*70}")
        logger.info("MÉTRICAS DE RENDIMIENTO")
        logger.info("="*70)
        logger.info(f"Tareas enviadas:       {metrics.tareas_enviadas:,}")
        logger.info(f"Tareas completadas:    {metrics.tareas_completadas:,}")
        logger.info(f"Tareas fallidas:       {metrics.tareas_fallidas:,}")
        logger.info(f"Tasa de éxito:         {metrics.tasa_exito():.1f}%")
        logger.info(f"Tiempo promedio/tarea: {metrics.tiempo_promedio_tarea:.2f}s")
        
        if metrics.errores_por_tipo:
            logger.info(f"\nErrores por tipo:")
            for tipo, count in sorted(metrics.errores_por_tipo.items(), key=lambda x: -x[1]):
                logger.info(f"  - {tipo}: {count}")
        
        logger.info("\n" + "="*70)
        logger.info("ARCHIVOS GENERADOS:")
        logger.info(f"  - {CACHE_FILE} ({len(cache.completed)} completadas)")
        logger.info(f"  - {CACHE_TASKS_FILE} ({len(cache.active_tasks)} activas)")
        logger.info(f"  - {METRICS_FILE}")
        logger.info(f"  - {LOG_FILE}")
        
        if not args.dry_run:
            logger.info("\nMONITOREO DE TAREAS:")
            logger.info("  https://code.earthengine.google.com/tasks")
        
        logger.info("\nESTRUCTURA EN GOOGLE DRIVE:")
        logger.info(f"  {DRIVE_ROOT}/")
        logger.info(f"  └── {DRIVE_COUNTRY}/")
        logger.info(f"      └── [NombreEstado]/")
        logger.info(f"          ├── [NombreEstado]_Estadisticas_2020.csv")
        logger.info(f"          └── M-[CodMun]/")
        logger.info(f"              ├── [CodMun]_NDVI_2020.tif")
        logger.info(f"              ├── [CodMun]_LandCover_2020.tif")
        logger.info(f"              └── ...")
        
        logger.info("\n" + "="*70)
        logger.info(f"✓ EXPORTACIÓN {'SIMULADA' if args.dry_run else 'COMPLETADA'}")
        logger.info("="*70 + "\n")
    
    return 0

# ======================================================================
# ENTRY POINT
# ======================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Exportación de imágenes satelitales y estadísticas por municipio',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  # Dry-run (validar sin ejecutar)
  python exportacion_municipios_mx.py --estado AGS --dry-run
  
  # Ejecutar un solo estado
  python exportacion_municipios_mx.py --estado JAL
  
  # Ejecutar todos los estados
  python exportacion_municipios_mx.py --all --max-concurrent 150
  
  # Modo verbose (debug)
  python exportacion_municipios_mx.py --estado AGS --verbose

CAMBIOS PRINCIPALES:
  ✅ Procesamiento por municipio en lugar de localidad
  ✅ Asset cambiado a 'Mexico'
  ✅ Estructura Drive: M-Dataset/M-Mexico/Estado/M-Municipio/
  ✅ Estadísticas guardadas en carpeta del estado
  ✅ Imágenes guardadas en carpeta M-[CodMun]
  ✅ Código simplificado y optimizado
        """
    )
    
    parser.add_argument(
        '--estado',
        type=str,
        help='Código del estado a procesar (ej: AGS, JAL, CDMX)'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Procesar todos los estados'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Modo simulación (no ejecuta task.start())'
    )
    
    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=MAX_CONCURRENT_TASKS,
        help=f'Máximo de tareas concurrentes (default: {MAX_CONCURRENT_TASKS})'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Modo verbose (DEBUG level en consola)'
    )
    
    args = parser.parse_args()
    
    # Validar argumentos
    if not args.estado and not args.all:
        parser.print_help()
        sys.exit(1)
    
    # Reconfigurar logging si verbose
    if args.verbose:
        logger = setup_logging(verbose=True)
    
    # Ejecutar
    try:
        exit_code = main(args)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.warning("\n⚠ Ejecución interrumpida por usuario")
        sys.exit(130)
    except Exception as e:
        logger.critical(f"Error fatal: {e}\n{traceback.format_exc()}")
        sys.exit(1)