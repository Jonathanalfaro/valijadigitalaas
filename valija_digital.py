import csv
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler

import pymupdf
import pytesseract
import pytz
from PIL import Image
from PyPDF2 import PdfMerger, PdfReader
from PyPDF2.errors import PdfReadError
from dotenv import load_dotenv
from thefuzz import fuzz
from watchdog.events import FileSystemEventHandler, DirCreatedEvent, FileCreatedEvent
from watchdog.observers import Observer

load_dotenv()

class ValijaDigitalConfig:
    def __init__(self):
        self.PATH_ARCHIVOS = os.getenv('PATH_ARCHIVOS')
        self.PATH_SUCURSALES = os.getenv('PATH_SUCURSALES')
        self.PROVEEDORES_CSV = os.getenv('PROVEEDORES_CSV')
        self.SUCURSALES_CSV = os.getenv('SUCURSALES_CSV')
        self.TESSERACT_PATH = os.getenv('TESSERACT_PATH')
        self.DATABASE_PATH = os.getenv('DATABASE_PATH')
        self.LOG_FILENAME = os.getenv('LOG_FILENAME')
        
        self.MESES = {
            '01': 'ENERO', '02': 'FEBRERO', '03': 'MARZO', '04': 'ABRIL',
            '05': 'MAYO', '06': 'JUNIO', '07': 'JULIO', '08': 'AGOSTO',
            '09': 'SEPTIEMBRE', '10': 'OCTUBRE', '11': 'NOVIEMBRE', '12': 'DICIEMBRE'
        }
        
        try:
            self.LOG_SIZE_IN_BYTES = int(os.getenv('LOG_SIZE_IN_BYTES', 1000000))
            self.NUMBER_OF_LOGS = int(os.getenv('NUMBER_OF_LOGS', 3))
        except ValueError:
            self.LOG_SIZE_IN_BYTES = 1000000
            self.NUMBER_OF_LOGS = 3
            
        pytesseract.pytesseract.tesseract_cmd = self.TESSERACT_PATH

class Logger:
    def __init__(self, config):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.INFO)
        stdout_handler.setFormatter(formatter)

        file_handler = RotatingFileHandler(
            config.LOG_FILENAME,
            encoding='utf-8',
            maxBytes=config.LOG_SIZE_IN_BYTES,
            backupCount=config.NUMBER_OF_LOGS
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(stdout_handler)

class DatabaseManager:
    def __init__(self, config):
        self.database_path = config.DATABASE_PATH
        
    def get_documento(self, nombre_documento, path_documento):
        documento = {}
        try:
            conn = sqlite3.connect(self.database_path)
            statement = '''SELECT d.id, d.name, d.current_path FROM documents_documents d where d.name = $1 and d.current_path = $2'''
            cursor_obj = conn.cursor()
            cursor_obj.execute(statement, [nombre_documento, path_documento])
            result_documento = cursor_obj.fetchone()
            if result_documento:
                documento = {
                    'id': result_documento[0],
                    'name': result_documento[1],
                    'current_path': result_documento[2]
                }
        except sqlite3.Error as error:
            logging.error(error)
        finally:
            conn.close()
        return documento

    def insertar_documento(self, documento):
        documento_dic = {}
        datetime_now = datetime.now(pytz.timezone('UTC'))
        try:
            conn = sqlite3.connect(self.database_path)
            statement = '''
                        insert into documents_documents (name, current_path, visible, size, uploaded_at) 
                        values ($1, $2, $3, $4, $5)
                        returning id, name, current_path, visible, size, uploaded_at
                    '''
            cursor_obj = conn.cursor()
            cursor_obj.execute(statement, [documento['name'], documento['current_path'], documento['visible'],
                                           documento['size'], datetime_now.strftime('%Y-%m-%d %H:%M:%S')])
            result_documento = cursor_obj.fetchone()
            if result_documento:
                documento_dic = {
                    'id': result_documento[0],
                    'name': result_documento[1],
                    'current_path': result_documento[2],
                    'visible': result_documento[3],
                    'size': result_documento[4],
                    'uploaded_at': result_documento[5]
                }
            conn.commit()
        except sqlite3.Error as error:
            logging.error(error)
        finally:
            conn.close()
        return documento_dic

    def update_size(self, documento):
        documento_dic = {}
        try:
            conn = sqlite3.connect(self.database_path)
            statement = '''
                        update documents_documents 
                        set size = $1
                        where id = $2
                        returning id, name, current_path, visible, size, uploaded_at
                    '''
            cursor_obj = conn.cursor()
            cursor_obj.execute(statement, [documento['size'], documento['id']])
            result_documento = cursor_obj.fetchone()
            if result_documento:
                documento_dic = {
                    'id': result_documento[0],
                    'name': result_documento[1],
                    'current_path': result_documento[2],
                    'visible': result_documento[3],
                    'size': result_documento[4],
                    'uploaded_at': result_documento[5]
                }
            conn.commit()
        except sqlite3.Error as error:
            logging.error(error)
        finally:
            conn.close()
        return documento_dic

    def insertar_log(self, log):
        resultado = False
        datetime_now = datetime.now(pytz.timezone('UTC'))
        try:
            conn = sqlite3.connect(self.database_path)
            statement = '''insert into logs_logs (log, documents_id,date) values ($1, $2, $3)'''
            cursor_obj = conn.cursor()
            cursor_obj.execute(statement, [log['log'], log['documents'], datetime_now.strftime('%Y-%m-%d %H:%M:%S')])
            conn.commit()
            if cursor_obj.rowcount == 1:
                resultado = True
        except sqlite3.Error as error:
            logging.error(error)
        finally:
            conn.close()
        return resultado

class CSVManager:
    def __init__(self, config):
        self.config = config
        
    def get_conf_csv(self):
        conf = {}
        try:
            with open('conf.csv', newline='') as csvfile:
                csv_conf = csv.reader(csvfile, skipinitialspace=True)
                for row in csv_conf:
                    conf.update({row[0]: row[1]})
            return conf
        except FileNotFoundError:
            logging.error('No se encontro el archivo conf.csv')
            return {}
        except Exception as e:
            logging.error(e)

    def get_proveedores_csv(self):
        proveedores = []
        try:
            with open(self.config.PROVEEDORES_CSV, newline='', encoding='utf-8') as csvfile:
                csv_proveedores = csv.reader(csvfile, skipinitialspace=True)
                try:
                    for row in csv_proveedores:
                        dict_proveedor = {
                            'name': row[0],
                            'scan_name': row[1],
                        }
                        proveedores.append(dict_proveedor)
                except IndexError:
                    pass
                except Exception as e:
                    logging.error(e)
                    pass
            return proveedores
        except FileNotFoundError:
            logging.error('No se encontro el archivo proveedores.csv')
            return []
        except Exception as e:
            logging.error(e)
            return []

    def get_sucursal_csv(self, numero_serie):
        try:
            with open(self.config.SUCURSALES_CSV, newline='', encoding='utf-8') as csvfile:
                csv_sucurlsales = csv.reader(csvfile, skipinitialspace=True)
                for row in csv_sucurlsales:
                    if row[0] == numero_serie:
                        return row[1], row[2]
                return None, None
        except FileNotFoundError:
            logging.error('No se encontró el archivo equipos_sucursal.csv')
            return None, None
        except Exception as e:
            logging.error(f'Error al leer el archivo equipos_sucursal.csv: {e}')
            return None, None

class PDFProcessor:
    def __init__(self, config, csv_manager):
        self.config = config
        self.csv_manager = csv_manager

    def get_size(self, path_documento):
        try:
            with open(path_documento, "rb") as f:
                try:
                    pdf_c = PdfReader(f, strict=False)
                    return len(pdf_c.pages)
                except PdfReadError:
                    logging.error('El archivo está vacío')
        except PermissionError:
            logging.error("No se puede leer el archivo, compruebe los permisos.")
        except FileNotFoundError:
            logging.error("No se encontró el archivo.")
        except Exception as e:
            logging.error(f'Error al leer el archivo: {e}')
        return 0

    def get_nombre_proveedor(self, path_documento):
        if not (os.path.exists(path_documento) and os.path.isfile(path_documento) and 'COMPLETO' not in path_documento):
            return None

        logging.debug(f'Obteniendo nombre de proveedor')
        conf_similitud = 75
        try:
            dif_conf = self.csv_manager.get_conf_csv()
            conf_similitud = int(dif_conf.get('similitud', 75))
        except (ValueError, KeyError):
            conf_similitud = 75

        try:
            doc = pymupdf.open(path_documento)
            pix_images = [page.get_pixmap(dpi=300) for page in doc]
            paginas_pdf = []
            for imagen in pix_images:
                data = imagen.tobytes('ppm')
                img = Image.frombytes('RGB', (imagen.width, imagen.height), data)
                paginas_pdf.append(img)
        except Exception as e:
            logging.error(f'Error al obtener el nombre del proveedor {e}')
            return None

        contrarecibo = self._find_contrarecibo(paginas_pdf)
        if not contrarecibo:
            logging.debug(f'No se encontró el contrarecibo en el documento')
            return None

        return self._extract_proveedor_name(contrarecibo, conf_similitud)

    def _find_contrarecibo(self, paginas_pdf):
        for pagina in paginas_pdf:
            pagina, imagen_encabezado = self._get_encabezado(pagina)
            texto_encabezado = pytesseract.image_to_data(imagen_encabezado, output_type=pytesseract.Output.DICT,
                                                         config='--psm 12 --oem 3 -c tessedit_char_whitelist=CONTRARECIBO')
            for text in texto_encabezado['text']:
                if 'CONTRARECIBO' in text:
                    return pagina
        return None

    def _get_encabezado(self, imagen):
        ancho, alto = imagen.size
        if ancho > alto:
            imagen = imagen.rotate(270, expand=True)
        alto2 = int(alto / 2)
        imagen_encabezado = imagen.crop((0, 0, ancho, alto2))
        return imagen, imagen_encabezado

    def _extract_proveedor_name(self, contrarecibo, conf_similitud):
        seccion_contrarecibo = self._get_seccion_contrarecibo(contrarecibo)
        texto_contrarecibo = pytesseract.image_to_data(seccion_contrarecibo, output_type=pytesseract.Output.DICT,
                                                       lang='eng',
                                                       config='--psm 12 --oem 3 -c tessedit_char_whitelist=PROVEEDOR ')
        
        palabras_coordenadas = zip(texto_contrarecibo['left'], texto_contrarecibo['top'],
                                   texto_contrarecibo['width'], texto_contrarecibo['height'],
                                   texto_contrarecibo['text'])
        
        seccion_proveedor = None
        for palabra in palabras_coordenadas:
            if 'PROVEEDOR' in palabra[4]:
                seccion_proveedor = palabra
                break
                
        if not seccion_proveedor:
            logging.debug(f'No se encontró el proveedor en el documento')
            return None

        imagen_proveedor = self._get_imagen_por_coordenadas(seccion_proveedor, seccion_contrarecibo)
        texto_proveedor = pytesseract.image_to_data(imagen_proveedor, output_type=pytesseract.Output.DICT,
                                                    config='--psm 12 --oem 3 -c tessedit_char_blacklist=,.:;:')

        return self._match_proveedor(texto_proveedor['text'], conf_similitud)

    def _get_seccion_contrarecibo(self, imagen):
        ancho, alto = imagen.size
        alto2 = int(alto / 2)
        return imagen.crop((0, 0, ancho, alto2))

    def _get_imagen_por_coordenadas(self, coordenadas, imagen):
        return imagen.crop((coordenadas[0] - 5, coordenadas[1] - 5, int(imagen.size[0] / 2), coordenadas[1] + coordenadas[3] + 5))

    def _match_proveedor(self, datos_proveedor, conf_similitud):
        datos_proveedor = [x for x in datos_proveedor if len(x) > 0]
        indice_nombre_proveedor = -1
        
        for i, dato in enumerate(datos_proveedor):
            match = re.search(r'(\d{4})', dato)
            if match:
                indice_nombre_proveedor = i + 1
                break
                
        if indice_nombre_proveedor > -1:
            nombre_proveedor = ''.join(datos_proveedor[indice_nombre_proveedor:])
        else:
            nombre_proveedor = None

        proveedores = self.csv_manager.get_proveedores_csv()
        nombres_proveedores = [x['name'] for x in proveedores]
        nombres_scan_proveedores = [x['scan_name'] for x in proveedores]
        
        if nombre_proveedor:
            nombre_proveedor_sanitizado = nombre_proveedor.replace(' ', '').replace(',', '').replace('.', '').lower()
            similitudes = []
            for k, nombre in enumerate(nombres_proveedores):
                nombre_sanitizado = nombre.replace(' ', '').replace(',', '').replace('.', '').lower()
                similitud = fuzz.ratio(nombre_sanitizado, nombre_proveedor_sanitizado)
                similitudes.append(similitud)
            
            maxima_similitud = max(similitudes) if similitudes else 0
            if maxima_similitud > conf_similitud:
                return nombres_scan_proveedores[similitudes.index(maxima_similitud)]
        else:
            for dato in datos_proveedor:
                if dato in nombres_scan_proveedores:
                    return dato
        return None

    def unir_documentos(self, path_destino, path_nuevo_archivo):
        logging.info('Uniendo documentos')
        resultado = False
        merger = PdfMerger()
        try:
            merger.append(path_destino)
            merger.append(path_nuevo_archivo)
            merger.write(path_destino)
            resultado = True
        except Exception as e:
            logging.error(f'Error al unir el documento {e}')
        finally:
            merger.close()
        return resultado

class FileManager:
    def __init__(self, config):
        self.config = config

    def mueve_archivo(self, path_archivo_origen, path_archivo_destino, overwrite=False):
        if not overwrite and os.path.exists(path_archivo_destino):
            path_archivo_destino = self._number_generator(path_archivo_destino)

        try:
            shutil.move(path_archivo_origen, path_archivo_destino)
            return path_archivo_destino
        except PermissionError:
            logging.info('Error. No se puede mover el archivo, compruebe los permisos.')
            time.sleep(3)
            logging.info('Intentando mover el archivo nuevamente')
            try:
                shutil.move(path_archivo_origen, path_archivo_destino)
                return path_archivo_destino
            except Exception:
                logging.error('Error. No se pudo mover el archivo, compruebe los permisos y elimínelo manualmente.')
        except (FileNotFoundError, shutil.Error) as e:
            logging.error(f'Error al mover archivo: {e}')
        return ''

    def _number_generator(self, path_archivo_destino):
        directorio, archivo = os.path.split(path_archivo_destino)
        nombre_archivo, extension = os.path.splitext(archivo)
        for i in range(1, 100000):
            path_aux = os.path.join(directorio, f'{nombre_archivo}_{i:06}{extension}')
            if not os.path.exists(path_aux):
                return path_aux

    def eliminar_archivo(self, path_archivo):
        try:
            os.remove(path_archivo)
            return True
        except (PermissionError, FileNotFoundError) as e:
            logging.error(f'Error al eliminar archivo: {e}')
            return False

class PathManager:
    def __init__(self, config, csv_manager, pdf_processor):
        self.config = config
        self.csv_manager = csv_manager
        self.pdf_processor = pdf_processor

    def crea_paths(self, path_archivo, nombre_archivo):
        path_carpeta = path_archivo.replace(self.config.PATH_ARCHIVOS, '')
        nombre_carpeta = path_carpeta.split(os.sep)[1]
        
        # Obtener flujo
        try:
            flujo = nombre_carpeta.split('-')[1]
        except IndexError:
            logging.error('Flujo no encontrado')
            raise ValueError

        # Obtener fecha
        match = re.search(r'(\d{4}-\d{2}-\d{2})', nombre_archivo)
        if not match:
            logging.error('Fecha no encontrada')
            raise ValueError
        fecha = match.group(1)

        # Obtener número de serie
        try:
            numero_serie = nombre_archivo.split('-')[0]
        except IndexError:
            logging.error('Nombre de archivo inválido')
            raise ValueError

        # Obtener sucursal
        sucursal, carpeta_raiz_sucursal = self.csv_manager.get_sucursal_csv(numero_serie)
        if not sucursal or not carpeta_raiz_sucursal:
            logging.error('No se encontró la sucursal')
            raise ValueError

        # Crear estructura de carpetas
        lista_fecha = fecha.split('-')
        try:
            anio, mes = lista_fecha[0], lista_fecha[1]
            mes_texto = self.config.MESES[mes]
        except (IndexError, KeyError):
            logging.error('Formato de fecha inválida.')
            raise ValueError

        # Obtener configuración
        dic_configuracion = self.csv_manager.get_conf_csv()
        separador_carpeta = dic_configuracion['separador_carpeta']
        separador_nombre = dic_configuracion['separador_nombre']
        complemento_nombre_completo = dic_configuracion['complemento_nombre_completo']

        # Crear paths
        path_sucursal = os.path.join(self.config.PATH_SUCURSALES, carpeta_raiz_sucursal)
        path_sucursal_flujo = os.path.join(path_sucursal, f'{sucursal}{separador_carpeta}{flujo}')
        path_anio = os.path.join(path_sucursal_flujo, anio)
        path_mes = os.path.join(path_anio, mes_texto)

        # Crear directorios
        self._crear_directorios([path_sucursal, path_sucursal_flujo, path_anio, path_mes])

        # Procesar según flujo
        return self._procesar_flujo(flujo, path_archivo, nombre_archivo, fecha, sucursal, 
                                   path_mes, separador_nombre, complemento_nombre_completo, path_carpeta)

    def _crear_directorios(self, paths):
        for path in paths:
            if not os.path.exists(path):
                try:
                    os.mkdir(path)
                    logging.debug(f'Creando carpeta {path}')
                except (PermissionError, FileExistsError) as e:
                    logging.error(f'Error al crear carpeta {path}: {e}')
                    raise ValueError

    def _procesar_flujo(self, flujo, path_archivo, nombre_archivo, fecha, sucursal, 
                       path_mes, separador_nombre, complemento_nombre_completo, path_carpeta):
        complemento_archivo = nombre_archivo.split(fecha)[1]
        carpeta_superior = path_carpeta.split(os.sep)[-2]
        complemento = ''

        if flujo == 'BANCOS':
            return [f'{sucursal}{separador_nombre}{fecha}{complemento_archivo}', path_mes, flujo, sucursal, complemento]
        
        elif flujo == 'CUENTAS POR PAGAR':
            return self._procesar_cuentas_por_pagar(carpeta_superior, path_mes, sucursal, 
                                                   separador_nombre, fecha, complemento_archivo, path_archivo)
        
        elif flujo == 'GASTOS':
            return self._procesar_gastos(carpeta_superior, nombre_archivo, path_mes, sucursal, 
                                       separador_nombre, fecha, complemento_archivo)
        
        return [None, None, flujo, sucursal, complemento]

    def _procesar_cuentas_por_pagar(self, carpeta_superior, path_mes, sucursal, separador_nombre, fecha, complemento_archivo, path_archivo):
        if carpeta_superior == 'DEVOLUCIONES':
            nombre_carpeta_superior = 'HOJAS DE DEVOLUCION - DVSR - DEVO'
            nuevo_path = os.path.join(path_mes, nombre_carpeta_superior)
            self._crear_directorios([nuevo_path])
            return [f'{sucursal}{separador_nombre}{fecha}{complemento_archivo}', nuevo_path, 'CUENTAS POR PAGAR', sucursal, '']
        
        elif carpeta_superior == 'FACTURAS Y CONTRARECIBOS':
            nombre_proveedor = self.pdf_processor.get_nombre_proveedor(path_archivo)
            if nombre_proveedor:
                logging.debug(f'Se encontró el nombre del proveedor {nombre_proveedor}')
                nuevo_nombre = f'{sucursal}{separador_nombre}{nombre_proveedor}-{fecha}{complemento_archivo}'
            else:
                logging.debug(f'No se encontró el nombre del proveedor.')
                nuevo_nombre = f'{sucursal}{separador_nombre}{fecha}{complemento_archivo}'
            
            nombre_carpeta_superior = 'FACTURAS DE PROVEEDORES Y CONTRA RECIBOS'
            nuevo_path = os.path.join(path_mes, nombre_carpeta_superior)
            self._crear_directorios([nuevo_path])
            return [nuevo_nombre, nuevo_path, 'CUENTAS POR PAGAR', sucursal, '']

    def _procesar_gastos(self, carpeta_superior, nombre_archivo, path_mes, sucursal, separador_nombre, fecha, complemento_archivo):
        match_incompleto = re.search(r'(\d{6}).pdf', nombre_archivo)
        extension_incompleto = '.pdf'
        complemento = ''
        
        if match_incompleto:
            extension_incompleto = f'_{match_incompleto.group(1)}.pdf'
            complemento = f'_{match_incompleto.group(1)}'

        if carpeta_superior == 'COMPRAS DE MERCANCIA':
            nombre_carpeta_superior = 'COMPRA DE MERCANCIA'
            nuevo_path = os.path.join(path_mes, nombre_carpeta_superior)
            self._crear_directorios([nuevo_path])
            return [f'{sucursal}{separador_nombre}COMPRA-{fecha}{extension_incompleto}', nuevo_path, 'GASTOS', sucursal, complemento]
        
        elif carpeta_superior == 'GASTOS OPERATIVOS':
            nuevo_path = os.path.join(path_mes, carpeta_superior)
            self._crear_directorios([nuevo_path])
            return [f'{sucursal}{separador_nombre}GASTO-{fecha}{complemento_archivo}', nuevo_path, 'GASTOS', sucursal, complemento]

class DocumentProcessor:
    def __init__(self, config, database_manager, pdf_processor):
        self.config = config
        self.database_manager = database_manager
        self.pdf_processor = pdf_processor

    def insertar_en_base_de_datos(self, documento):
        logging.debug(f'Insertando en la base de datos')
        path_documento_completo = os.path.join(documento['current_path'], documento['name'])
        num_paginas = self.pdf_processor.get_size(path_documento_completo)
        documento['size'] = num_paginas
        n_documento = self.database_manager.insertar_documento(documento)
        if n_documento:
            logging.debug(f'El documento se insertó correctamente en la base de datos')
            log = {
                "log": f"Se creó el documento {n_documento['name']}.",
                "documents": n_documento['id']
            }
            self.database_manager.insertar_log(log)
            return True
        else:
            logging.error(f'Error al insertar el documento en la base de datos')
            return False

class FileObserver(FileSystemEventHandler):
    def __init__(self, config, csv_manager, pdf_processor, file_manager, path_manager, document_processor, database_manager):
        self.config = config
        self.csv_manager = csv_manager
        self.pdf_processor = pdf_processor
        self.file_manager = file_manager
        self.path_manager = path_manager
        self.document_processor = document_processor
        self.database_manager = database_manager

    def on_created(self, event):
        if isinstance(event, FileCreatedEvent):
            if self.config.PATH_SUCURSALES not in event.src_path:
                self._process_file(event.src_path)

    def _process_file(self, path_nuevo_archivo):
        ruta_archivo, nombre_archivo = os.path.split(path_nuevo_archivo)
        nombre, extension = os.path.splitext(nombre_archivo)
        logging.info(f"nuevo archivo {path_nuevo_archivo}")
        
        if extension.lower() != '.pdf':
            logging.info('Omitiendo archivo')
            return

        try:
            nombre, nuevo_path, flujo, sucursal, complemento = self.path_manager.crea_paths(path_nuevo_archivo, nombre_archivo)
        except ValueError:
            logging.error('No se pudo crear el path.')
            return
        except Exception:
            logging.error('No se pudo procesar el documento pues no está en una carpeta conocida.')
            return

        if flujo in ['BANCOS', 'CUENTAS POR PAGAR']:
            self._process_simple_file(path_nuevo_archivo, nuevo_path, nombre)
        elif flujo == 'GASTOS':
            self._process_gastos_file(path_nuevo_archivo, nuevo_path, nombre, complemento)
        else:
            logging.error('Flujo desconocido.')
            return

        logging.info(f'Se procesó el documento {path_nuevo_archivo}')

    def _process_simple_file(self, path_nuevo_archivo, nuevo_path, nombre):
        path_destino = os.path.join(nuevo_path, nombre)
        path = self.file_manager.mueve_archivo(path_nuevo_archivo, path_destino, overwrite=False)
        if path:
            current_path, name = os.path.split(path)
            doc = {
                'name': name,
                'current_path': current_path,
                'visible': True,
            }
            self.document_processor.insertar_en_base_de_datos(doc)

    def _process_gastos_file(self, path_nuevo_archivo, nuevo_path, nombre, complemento):
        nombre_arch, extension_arch = os.path.splitext(nombre)
        dic_configuracion = self.csv_manager.get_conf_csv()
        complemento_nombre_completo = dic_configuracion['complemento_nombre_completo']
        nombre_completo = f'{nombre_arch}{complemento_nombre_completo}{extension_arch}'
        
        if complemento != '':
            nombre_completo = nombre_completo.replace(f'{complemento}.pdf', '.pdf')
        
        path_destino = os.path.join(nuevo_path, nombre_completo)
        
        if os.path.exists(path_destino):
            self._merge_documents(path_destino, path_nuevo_archivo)
        else:
            path = self.file_manager.mueve_archivo(path_nuevo_archivo, path_destino, overwrite=True)
            if path:
                doc = {
                    'name': nombre_completo,
                    'current_path': nuevo_path,
                    'visible': True,
                }
                self.document_processor.insertar_en_base_de_datos(doc)

    def _merge_documents(self, path_destino, path_nuevo_archivo):
        resultado_unir = self.pdf_processor.unir_documentos(path_destino, path_nuevo_archivo)
        if resultado_unir:
            current_path, name = os.path.split(path_destino)
            doc = self.database_manager.get_documento(name, current_path)
            if doc:
                doc['size'] = self.pdf_processor.get_size(path_destino)
                self.database_manager.update_size(doc)
                _, archivo_unido = os.path.split(path_nuevo_archivo)
                log = {
                    'log': f'Se unió el documento {archivo_unido}',
                    'documents': doc['id']
                }
                self.database_manager.insertar_log(log)
            
            # Intentar eliminar archivo
            if not self.file_manager.eliminar_archivo(path_nuevo_archivo):
                logging.info('No se pudo eliminar el archivo. Reintentando...')
                time.sleep(2)
                if not self.file_manager.eliminar_archivo(path_nuevo_archivo):
                    logging.info('No se pudo eliminar el archivo. Eliminar manualmente.')
                else:
                    logging.info('Se eliminó el archivo.')

class ValijaDigitalApp:
    def __init__(self):
        self.config = ValijaDigitalConfig()
        self.logger = Logger(self.config)
        self.database_manager = DatabaseManager(self.config)
        self.csv_manager = CSVManager(self.config)
        self.pdf_processor = PDFProcessor(self.config, self.csv_manager)
        self.file_manager = FileManager(self.config)
        self.path_manager = PathManager(self.config, self.csv_manager, self.pdf_processor)
        self.document_processor = DocumentProcessor(self.config, self.database_manager, self.pdf_processor)
        
        self.file_observer = FileObserver(
            self.config, self.csv_manager, self.pdf_processor, 
            self.file_manager, self.path_manager, self.document_processor, self.database_manager
        )

    def run(self):
        observer = Observer()
        observer.schedule(self.file_observer, path=self.config.PATH_ARCHIVOS, recursive=True)
        observer.start()
        logging.info('Observando directorio: %s', self.config.PATH_ARCHIVOS)
        
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            observer.stop()
        finally:
            observer.join()

def main():
    app = ValijaDigitalApp()
    app.run()

if __name__ == "__main__":
    main()