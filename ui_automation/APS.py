from pywinauto.application import Application
from tqdm import tqdm
from time import sleep

app = Application(backend="win32").start(r"C:\TSI\Aerosol Instrument Manager\Aim.exe")
window = app.window()

unwanted_folder_items = ["Header Control", "Vertical", "Horizontal"]
visited_dates = []
folder_items = [None]

while folder_items:
    window.menu_select("File->Open")
    open_dlg = app["Open Instrument Associated Data Files"]
    open_btn = open_dlg.Open
    folder_view = open_dlg.FolderView
    folder_combobox = open_dlg.LookinComboBox
    file_type_combobox = open_dlg.FilesoftypeComboBox
    folder_name = "APS"
    file_type = "APS 3321 Data file (*.A21)"

    if folder_combobox.selected_text() != folder_name:
        folder_combobox.select("Local Disk (D:)")
        folder_view.select(folder_name)
    if file_type_combobox.selected_text() != file_type:
        file_type_combobox.select(file_type)

    open_btn.click()

    if folder_items[0] is None:
        folder_items = folder_view.texts()[1::4]

    file_name = folder_items.pop()
    if file_name and file_name not in unwanted_folder_items:
        date = file_name.split('_')[0][1:]

        if date not in visited_dates:
            folder_view.select(file_name)
            open_btn.click()

            popup = app.window(title="File Question?")
            yes_btn = popup.YesButton
            yes_btn.click()
            sleep(5)
            
            # DO STUFF

            window.menu_select("File->Export")
            export_dlg = app["Export Parameters"]

            # Data Types
            # syntax: {title: is_checked}
            # is_checked: True = checked; False = unchecked
            checkboxes = {
                "Aerodynamic": True,
                "Aerodynamic Raw Data": False,
                "Side Scatter": False,
                "Side Scatter Raw Data": False,
                "Correlated": False
            }

            for title in checkboxes:
                checkbox = export_dlg[title]
                if not checkbox.is_checked and checkboxes[title]:
                    checkbox.check()
                elif checkbox.is_checked and not checkboxes[title]:
                    checkbox.uncheck()

            # Units and Weights
            unit_btn = export_dlg["dW/dlogDpRadioButton"]
            unit_btn.click()

            # Delimiter
            delimiter_btn = export_dlg["Comma"]
            delimiter_btn.click()

            # Orientation
            orientation_btn = export_dlg["Row"]
            orientation_btn.click()

            ok_btn = export_dlg.OK
            ok_btn.click()

            export_as_dlg = app["Export As"]
            # TODO: check which directory should be saved in
            save_btn = export_as_dlg["Save"]
            save_btn.click()
