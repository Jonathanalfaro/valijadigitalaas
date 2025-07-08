import win32serviceutil
import win32service
import win32event
import servicemanager
import sys
import os

# Agregar el directorio del script al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from valija_digital import ValijaDigitalApp

class ValijaDigitalService(win32serviceutil.ServiceFramework):
    _svc_name_ = "ValijaDigital"
    _svc_display_name_ = "Servicio Valija Digital"
    _svc_description_ = "Servicio de monitoreo Valija Digital"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
        self.app = None

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)
        try:
            import logging
            logging.info('Deteniendo servicio ValijaDigital')
        except:
            pass

    def SvcDoRun(self):
        servicemanager.LogMsg(servicemanager.EVENTLOG_INFORMATION_TYPE,
                              servicemanager.PYS_SERVICE_STARTED,
                              (self._svc_name_, ''))
        self.main()

    def main(self):
        try:
            import logging
            logging.basicConfig(filename='C:\\temp\\valija_service.log', level=logging.DEBUG, 
                              format='%(asctime)s - %(levelname)s - %(message)s')
            logging.info('Iniciando servicio ValijaDigital')
            
            # Cambiar al directorio de trabajo
            os.chdir(os.path.dirname(os.path.abspath(__file__)))
            
            self.app = ValijaDigitalApp()
            
            # Ejecutar en un hilo separado
            import threading
            self.app_thread = threading.Thread(target=self.app.run)
            self.app_thread.daemon = False
            self.app_thread.start()
            
            logging.info('Servicio iniciado correctamente')
            
            # Esperar hasta que se detenga el servicio
            win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)
            
        except Exception as e:
            import traceback
            error_msg = f"Error en el servicio: {str(e)}\n{traceback.format_exc()}"
            try:
                with open('C:\\temp\\valija_error.log', 'a') as f:
                    f.write(f"{error_msg}\n")
            except:
                pass
            servicemanager.LogErrorMsg(error_msg)

if __name__ == '__main__':
    if len(sys.argv) == 1:
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(ValijaDigitalService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        win32serviceutil.HandleCommandLine(ValijaDigitalService)