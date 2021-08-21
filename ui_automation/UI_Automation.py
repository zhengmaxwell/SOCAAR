from pywinauto.application import Application
from pywinauto.keyboard import send_keys
from abc import ABC, abstractmethod

class UIAutomation(ABC):

    def __init__(self, app_path: str, window: str, wait: bool=True, backend: str="win32") -> None:
        
        self._app_path = app_path
        self._backend = backend
        self.wait = "exists enabled visible ready" if wait else None
        self.app = None
        self.window = None

        self._connect()

    def _connect(self, app_path: str, backend: str) -> None:

        self.app = Application(backend=backend).start(app_path)
        self.window = self.app[window]

    @abstractmethod
    def export_data(self) -> None:
        pass

    @abstractmethod
    def upload_data(self) -> None:
        pass