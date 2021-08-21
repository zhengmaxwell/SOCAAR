from UI_Automation import UIAutomation
from pywinauto.application import Application
from tqdm import tqdm


class FMPS(UIAutomation):

    def __init__(self, app_path: str, window: str) -> None:

        super().__init__(app_path, window)

    
    def export_data(self) -> None:

        unwanted_folder_items = ["Header Control", "Vertical", "Horizontal"]
        folder_items = [None]

        with tqdm(desc="FMPS") as pbar:
            while folder_items:
                self.window.menu_select("File->Open")
                open_dlg = self.app["Open"]
                open_dlg.wait(self.wait)
                open_btn = open_dlg.OpenButton3
                folder_view = open_dlg.FolderView
                folder_combobox = open_dlg.LookinComboBox
                folder_name = "FMPS"

                if folder_combobox.selected_text() != folder_name:
                    folder_combobox.select("Local Disk (D:)")
                    folder_view.select(folder_name)

                open_btn.click()

                if folder_items[0] is None:
                    folder_items = folder_view.texts()[1::4]
                    pbar.total = len(folder_items)
                    pbar.refresh()

                file_name = folder_items.pop()
                if file_name and file_name not in unwanted_folder_items:
                    pbar.set_postfix(file=file_name)
                    folder_view.select(file_name)
                    open_btn.click()
                    
                    window = self.app[f"Fast Mobility Particle Sizer - {file_name}"]
                    window.wait(self.wait)
                    window.menu_select("File->Export")
                    export_dlg = self.app["Export Data Options"]
                    export_dlg.wait(self.wait)
                    
                    # Data Types
                    # syntax: {title: is_checked}
                    # is_checked: True = checked; False = unchecked
                    checkboxes = {
                        "Concentration" : True,
                        "Surface": True,
                        "Volume": True,
                        "Mass": True,
                        "Raw Data              (Instrument Record)": False, # extra space not a typo
                        "Total Concentration": True,
                        "Electrometer Current": False,
                        "Sample Temp.": False,
                        "Pressure": False,
                        "Analog Input": False,
                        "Min - Max  Concentration  Limits": False
                    }          

                    for title in checkboxes:
                        checkbox = export_dlg[title]
                        if not checkbox.is_checked() and checkboxes[title]:
                            checkbox.check()
                        elif checkbox.is_checked() and not checkboxes[title]:
                            checkbox.uncheck()

                    units_btn = export_dlg["Normalized (dW/dlogDp)"]
                    if not units_btn.is_checked():
                        units_btn.click()

                    # Time Range and Resolution
                    range_btn = export_dlg.EntireRunButton
                    range_btn.click()

                    interval_combobox = export_dlg.ComboBoxTaskBar
                    desired_text = "60.0 sec"
                    if interval_combobox.selected_text() != desired_text:
                        interval_combobox.select(desired_text)

                    display_time_btn = export_dlg["hh:mm:ss"]
                    if not display_time_btn.is_checked():
                        display_time_btn.click()

                    # Output File Type
                    file_type_btn = export_dlg["Text  (*.txt)"]
                    if not file_type_btn.is_checked():
                        file_type_btn.click()

                    delimiter_btn = export_dlg["Comma"]
                    if not delimiter_btn.is_checked():
                        delimiter_btn.click()
                    
                    ok_btn = export_dlg.OKButton
                    ok_btn.click()
                    pbar.update()

        window.close()

    def upload_data(self) -> None:
        raise NotImplementedError


if __name__ == "__main__":

    app_path = r"C:\TSI\Fast Mobility Particle Sizer\fmps.exe"
    window = "Fast Mobility Particle Sizer"
    fmps = FMPS(app_path, window)
    fmps.export_data()