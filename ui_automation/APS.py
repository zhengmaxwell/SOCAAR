from UIAutomation import UIAutomation
from pywinauto.application import Application
from pywinauto.keyboard import send_keys
from tqdm import tqdm


class APS(UIAutomation):

    def __init__(self, app_path: str, window: str) -> None:

        super().__init__(app_path, window)

    def export_data(self) -> None:

        unwanted_folder_items = ["Header Control", "Vertical", "Horizontal"]
        visited_dates = []
        folder_items = [None]

        with tqdm(desc="APS") as pbar:
            while folder_items:
                self.window.menu_select("File->Open")
                open_dlg = self.app["Open Instrument Associated Data Files"]
                open_dlg.wait(self.wait)
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
                    pbar.total = len(folder_items)
                    pbar.refresh()

                file_name = folder_items.pop() # TODO: check which files are needed
                if file_name and file_name not in unwanted_folder_items:
                    date = file_name.split('_')[0][1:]

                    if date not in visited_dates:
                        # remove other duplicate files
                        folder_items = list(filter(lambda f: f.split('_')[0][1:] != date, folder_items))
                        pbar.set_postfix(file=file_name)
                        visited_dates.append(date)
                        folder_view.select(file_name)
                        open_btn.click()

                        popup = self.app.window(title="File Question?")
                        yes_btn = popup.YesButton
                        yes_btn.click()
                        
                        window = self.app[f"Aerosol Instrument Manager - {file_name}"]
                        window.wait(self.wait)
                        samples_list = window.ListView
                        samples_list.wait(self.wait, timeout=10) # TODO: check timeout
                        samples_list.select(samples_list.texts()[1::4][0]) # select first item
                        send_keys("^a^c") # select all
                        
                        window.menu_select("File->Export")
                        export_dlg = self.app["Export Parameters"]
                        export_dlg.wait(self.wait)

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
                            if not checkbox.is_checked() and checkboxes[title]:
                                checkbox.check()
                            elif checkbox.is_checked() and not checkboxes[title]:
                                checkbox.uncheck()

                        # Units and Weights
                        unit_btn = export_dlg["dW/dlogDpRadioButton"]
                        if not unit_btn.is_checked():
                            unit_btn.click()

                        # Delimiter
                        delimiter_btn = export_dlg["Comma"]
                        if not delimiter_btn.is_checked():
                            delimiter_btn.click()

                        # Orientation
                        orientation_btn = export_dlg["Row"]
                        if not orientation_btn.is_checked():
                            orientation_btn.click()

                        ok_btn = export_dlg.OK
                        ok_btn.click()

                        export_as_dlg = self.app["Export As"]
                        # TODO: check which directory should be saved in
                        save_btn = export_as_dlg["Save"]
                        save_btn.click()
                        pbar.update()

            window.close()

    def upload_data(self) -> None:
        raise NotImplementedError


if __name__ == "__main__":

    app_path = r"C:\TSI\Aerosol Instrument Manager\Aim.exe"
    window = "Aerosol Instrument Manager"
    aps = APS(app_path, window)
    aps.export_data()