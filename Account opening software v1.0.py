'''功能特点：
- 支持多文件格式（CSV/Excel）
- 显示匹配记录的来源文件和具体工作表
- 不区分大小写检索
- 显示完整的关联字段信息
- 支持中文字符处理
- 自动跳过无法读取的文件

注意事项：
1. 确保数据文件编码为UTF-8（尤其是CSV文件）
2. 大型Excel文件（>10MB）加载可能需要较长时间
3. 结果中的空字段会显示为NaN，可以通过修改代码处理空值显示

这个解决方案可以满足跨表格关联数据的检索需求，通过Python的pandas库高效处理表格数据，并提供清晰的来源追踪功能。用户可以根据需要扩展支持更多文件格式或添加结果导出功能。'''

import os
import pandas as pd
from pathlib import Path


class TableSearcher:
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.results = []

    def search_in_tables(self, keyword):
        """在指定目录的所有表格文件中搜索关键词"""
        self.results.clear()

        # 遍历目录下所有支持的文件
        for file_path in self.data_dir.glob('*.*'):
            try:
                if file_path.suffix.lower() == '.csv':
                    self._process_csv(file_path, keyword)
                elif file_path.suffix.lower() in ('.xls', '.xlsx'):
                    self._process_excel(file_path, keyword)
            except Exception as e:
                print(f"读取文件{file_path.name}时出错: {str(e)}")

        return self.results

    def _process_csv(self, file_path, keyword):
        """处理CSV文件"""
        df = pd.read_csv(file_path, dtype=str)
        self._check_dataframe(df, file_path.name, "CSV")

    def _process_excel(self, file_path, keyword):
        """处理Excel文件"""
        xls = pd.ExcelFile(file_path)
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
            self._check_dataframe(df, file_path.name, sheet_name)

    def _check_dataframe(self, df, filename, sheetname):
        """检查数据框中的匹配项"""
        for _, row in df.iterrows():
            if any(row.astype(str).str.contains(keyword, case=False, na=False)):
                # 记录匹配结果及来源信息
                record = {
                    'file': filename,
                    'sheet': sheetname,
                    'data': row.to_dict()
                }
                self.results.append(record)


def main():
    # 配置数据目录（用户需要修改为自己的路径）
    DATA_DIR = "./data"  # 存放表格文件的目录

    if not os.path.exists(DATA_DIR):
        print(f"错误：数据目录不存在 {DATA_DIR}")
        return

    searcher = TableSearcher(DATA_DIR)

    while True:
        keyword = input("\n请输入要检索的关键词（输入q退出）: ").strip()
        if keyword.lower() == 'q':
            break

        results = searcher.search_in_tables(keyword)

        if not results:
            print("没有找到匹配结果")
            continue

        print(f"\n找到 {len(results)} 条相关记录:")
        for i, record in enumerate(results, 1):
            print(f"\n【记录{i}】文件：{record['file']} | 工作表：{record['sheet']}")
            for k, v in record['data'].items():
                print(f"{k}: {v}")


if __name__ == "__main__":
    main()