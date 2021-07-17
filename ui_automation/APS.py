from pywinauto.application import Application
from tqdm import tqdm

app = Application(backend="uia").start(r"C:\TSI\Aerosol Instrument Manager\Aim.exe")
window = app.window()










