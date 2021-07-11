from pywinauto.application import Application

app = Application(backend="uia").start(r"C:\TSI\Fast Mobility Particle Sizer\fmps.exe")
window = app.window()

# assumes opens 'FMPS' folder; TODO: check this

unwanted_folder_items = ["Header Control", "Vertical", "Horizontal"]
file_found = True
while file_found:
    file_found = False
    window.menu_select("File->Open")
    open_dlg = window.Open
    open_btn = open_dlg.OpenButton3
    folder_view = open_dlg.FolderView

    for file_name in folder_view.children_texts():
        if file_name and file_name not in unwanted_folder_items:
            file_select = folder_view[file_name]
            file_select.select()
            open_btn.click()
            
            window.menu_select("File->Export")
            export_dlg = window.ExportDataOptions

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
                checkbox = export_dlg.window(title=title, control_type="CheckBox")
                if checkbox.get_toggle_state() != checkboxes[title]:
                    checkbox.toggle()

            units_btn = export_dlg.window(title="Normalized (dW/dlogDp)", control_type="RadioButton")
            if not units_btn.is_selected():
                units_btn.select()


            # Time Range and Resolution
            range_btn = export_dlg.EntireRunButton
            range_btn.click()

            interval_textbox = export_dlg.ComboBoxTaskBar.Edit
            desired_text = "60.0 sec"
            if interval_textbox.get_value() != desired_text:
                interval_textbox.set_edit_text(desired_text)

            display_time_btn = export_dlg.window(title="hh:mm:ss", control_type="RadioButton")
            if not display_time_btn.is_selected():
                display_time_btn.select()


            # Output File Type
            file_type_btn = export_dlg.window(title="Text  (*.txt)", control_type="RadioButton")
            if not file_type_btn.is_selected():
                file_type_btn.select()

            delimiter_btn = export_dlg.window(title="Comma", control_type="RadioButton")
            if not delimiter_btn.is_selected():
                delimiter_btn.select()

            # Output File Name
            # assumes this is preset based on settings above # TODO: check this

            ok_btn = export_dlg.OKButton
            ok_btn.click()
            
            unwanted_folder_items.append(file_name)
            file_found = True
