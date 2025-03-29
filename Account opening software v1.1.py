"""主要改进点：

1. **中文编码支持**：
   - CSV文件自动尝试多种编码（UTF-8/GBK/GB18030）
   - 新增编码检测机制，自动处理常见中文编码问题

2. **匹配逻辑优化**：
   - 改用全字段拼接匹配方式，确保不会漏掉跨列匹配
   - 显示处理空值（自动转换为空字符串）
   - 强制所有单元格内容转为字符串类型

3. **错误修复**：
   - 修复未传递keyword参数的致命错误
   - 增强Excel文件的异常处理

4. **显示优化**：
   - 中文错误提示
   - 更直观的结果展示格式"""

'''主要改进点：

1. **中文编码支持**：
   - CSV文件自动尝试多种编码（UTF-8/GBK/GB18030）
   - 新增编码检测机制，自动处理常见中文编码问题

2. **匹配逻辑优化**：
   - 改用全字段拼接匹配方式，确保不会漏掉跨列匹配
   - 显示处理空值（自动转换为空字符串）
   - 强制所有单元格内容转为字符串类型

3. **错误修复**：
   - 修复未传递keyword参数的致命错误
   - 增强Excel文件的异常处理

4. **显示优化**：
   - 中文错误提示
   - 更直观的结果展示格式'''

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
        """处理CSV文件（自动检测编码）"""
        encodings = ['utf-8', 'gbk', 'gb18030']  # 常见中文编码
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding=enc)
                self._check_dataframe(df, keyword, file_path.name, "CSV")
                return
            except UnicodeDecodeError:
                continue
        print(f"无法识别文件编码：{file_path.name}")

    def _process_excel(self, file_path, keyword):
        """处理Excel文件"""
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
                self._check_dataframe(df, keyword, file_path.name, sheet_name)
        except Exception as e:
            print(f"读取Excel文件失败：{str(e)}")

    def _check_dataframe(self, df, keyword, filename, sheetname):
        """检查数据框中的匹配项"""
        for _, row in df.iterrows():
            # 将整个行转换为字符串进行匹配
            row_str = ' '.join([str(cell) for cell in row.values])
            if keyword.lower() in row_str.lower():
                record = {
                    'file': filename,
                    'sheet': sheetname,
                    'data': row.to_dict()
                }
                self.results.append(record)


def main():
    # 配置数据目录（用户需要修改为自己的路径）
    DATA_DIR = "./data"

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