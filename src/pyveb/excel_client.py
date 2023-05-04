from abc import ABC, abstractmethod
import xlsxwriter
import pandas as pd
import shutil,os, contextlib, time
import logging

class ExcelGenerator(ABC):

    def __init__(self, file_name:str):
        self.file_name = file_name

    @abstractmethod
    def _generate_excel(self, file_name):
        pass

    def generate(self) -> str:
        download_path = "./tmp_data/"
        timestamp = round(time.time(), 4)
        with contextlib.suppress(Exception):
            shutil.rmtree(download_path)
        if not os.path.exists(download_path):
            os.mkdir(download_path)
        local_file = f"{download_path}{timestamp}_{self.file_name}"
        logging.warning(local_file)
        self._generate_excel(local_file)
        return local_file

class DefaultExcel(ExcelGenerator):

    def __init__(self, df:pd.DataFrame, file_name:str) -> None:
        """
            Generates a default excel file from a pandas dataframe. 

            Call the 'generate' method on the instance to generate the file and this method also returns the name of the local file holding the excel
        """
        self.df = df
        super().__init__(file_name)

    def _generate_excel(self, file_name):
        workbook = xlsxwriter.Workbook(file_name, {'nan_inf_to_errors': True})
        sheet = workbook.add_worksheet()
        header_row = 0
        sheet.freeze_panes(header_row + 1, 0)
        header_fmt = workbook.add_format({'bg_color': '#BFD2E2'})
        for col_idx, header in enumerate(list(self.df)):
            sheet.write(header_row, col_idx, header, header_fmt)
        # There are a lot of NaN columns in the DF, excel displays this as an error for numerical columns, so we fill it with 0 instead.
        offset = 1
        magic_num = -0xDEADBEEF
        data = self.df.fillna(magic_num)
        for row_idx, row in data.iterrows():
            for col_idx, col in enumerate(row):
                if row_idx == header_row:         # set column lengths based on header length with a minimum of 20
                    col_length = len(str(col))
                    col_length = max(col_length, 20)
                    sheet.set_column(col_idx, col_idx, col_length * 1.25 )
                if col == magic_num:
                    continue
                sheet.write(row_idx + offset, col_idx, col)
        workbook.close()
        return 

