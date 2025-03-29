"""
智能表格检索系统 v5.0
核心改进：
1. 新增文件格式统一预处理模块
2. 实现数据标准化流水线
3. 支持自动修复常见格式问题
"""

import os
import sys
import json
import logging
import threading
import pandas as pd
from pathlib import Path
from tkinter import *
from tkinter import ttk, filedialog, messagebox
from pypinyin import lazy_pinyin, Style
import jaro
import chardet
import openpyxl
from openpyxl import load_workbook

# 配置日志系统
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class DataNormalizer:
    """数据标准化处理器"""

    @staticmethod
    def normalize_columns(df):
        """统一列名格式"""
        # 中文列名标准化
        column_map = {
            '姓名': ['名字', '姓名', 'name', '姓名'],
            '电话': ['手机', '电话', '联系方式', 'phone'],
            '部门': ['部门', '单位', 'department']
        }

        new_columns = []
        for col in df.columns:
            col_lower = str(col).lower()
            for standard, variants in column_map.items():
                if any(v.lower() in col_lower for v in variants):
                    new_columns.append(standard)
                    break
            else:
                new_columns.append(col)
        df.columns = new_columns
        return df

    @staticmethod
    def normalize_data(df):
        """统一数据类型"""
        # 去除首尾空格
        df = df.applymap(lambda x: str(x).strip() if isinstance(x, str) else x)
        # 处理空值
        df.replace(['', 'NA', 'N/A', 'NaN'], pd.NA, inplace=True)
        # 统一日期格式
        date_columns = [col for col in df.columns if '日期' in col or 'date' in col.lower()]
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        return df


class FileLoader:
    """统一文件加载器"""

    def __init__(self):
        self.logger = logging.getLogger('Loader')
        self.encodings = ['utf-8-sig', 'gb18030', 'utf-16', 'big5']

    def load_file(self, file_path):
        """统一加载入口"""
        ext = file_path.suffix.lower()
        try:
            if ext == '.csv':
                return self.load_csv(file_path)
            elif ext in ('.xls', '.xlsx', '.et', '.ods'):
                return self.load_excel(file_path)
            else:
                self.logger.warning(f"不支持的文件格式: {ext}")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"文件加载失败: {file_path} - {str(e)}")
            return pd.DataFrame()

    def detect_encoding(self, file_path):
        """增强编码检测"""
        try:
            with open(file_path, 'rb') as f:
                bom = f.read(4)
                if bom.startswith(b'\xef\xbb\xbf'):
                    return 'utf-8-sig'
                if bom.startswith(b'\xff\xfe'):
                    return 'utf-16'
                result = chardet.detect(f.read(100000))
                return result['encoding'] or 'utf-8'
        except Exception as e:
            self.logger.error(f"编码检测异常: {str(e)}")
            return 'utf-8'

    def load_csv(self, file_path):
        """CSV加载器"""
        encoding = self.detect_encoding(file_path)
        for enc in [encoding] + self.encodings:
            try:
                df = pd.read_csv(
                    file_path,
                    encoding=enc,
                    engine='python',
                    on_bad_lines='warn',
                    dtype=str,
                    keep_default_na=False
                )
                return DataNormalizer.normalize_data(df)
            except Exception as e:
                continue
        return pd.DataFrame()

    def load_excel(self, file_path):
        """Excel加载器"""
        try:
            # 读取所有sheet页并合并
            sheets = pd.read_excel(file_path, sheet_name=None, engine='openpyxl', dtype=str)
            df = pd.concat(sheets.values(), ignore_index=True)
            return DataNormalizer.normalize_columns(df.pipe(DataNormalizer.normalize_data))
        except Exception as e:
            self.logger.error(f"Excel读取失败: {str(e)}")
            return pd.DataFrame()


class TableSearcher:
    """增强版搜索器"""

    def __init__(self):
        self.loader = FileLoader()
        self.homophone_map = self.load_homophones()
        self.logger = logging.getLogger('Searcher')
        self.cache = {}

    def load_homophones(self):
        """加载同音字配置"""
        try:
            with open('Account opening software/homophones.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {'楠': ['南', '男'], '陈': ['晨', '尘']}

    def preprocess_data(self, df):
        """数据预处理"""
        # 合并所有列用于搜索
        df['_search_text'] = df.astype(str).apply('|'.join, axis=1)
        return df

    def generate_variants(self, keyword):
        """生成搜索变体"""
        variants = {keyword, keyword.lower(), keyword.upper()}
        try:
            # 拼音处理
            variants.add(''.join(lazy_pinyin(keyword, style=Style.NORMAL)))
            variants.add(''.join(lazy_pinyin(keyword, style=Style.FIRST_LETTER)))
            # 同音字替换
            for i, char in enumerate(keyword):
                for homophone in self.homophone_map.get(char, []):
                    variants.add(keyword[:i] + homophone + keyword[i + 1:])
        except Exception as e:
            self.logger.error(f"变体生成失败: {str(e)}")
        return variants

    def search_file(self, file_path, keyword, progress_callback):
        """标准化搜索流程"""
        try:
            cache_key = f"{file_path}-{keyword}"
            if cache_key in self.cache:
                return self.cache[cache_key]

            # 统一加载和预处理
            df = self.loader.load_file(file_path).pipe(self.preprocess_data)
            if df.empty:
                return []

            results = []
            variants = self.generate_variants(keyword)
            search_texts = df['_search_text'].str.lower().tolist()

            for idx, text in enumerate(search_texts):
                for variant in variants:
                    similarity = jaro.jaro_winkler_metric(variant.lower(), text)
                    if similarity >= 0.7:
                        record = {
                            'file': file_path.name,
                            'content': df.iloc[idx].to_dict(),
                            'similarity': f"{similarity:.0%}",
                            'pattern': variant
                        }
                        results.append(record)
                        break  # 每个记录只匹配一次

            self.cache[cache_key] = results
            progress_callback(len(results))
            return results
        except Exception as e:
            self.logger.error(f"搜索异常: {str(e)}")
            return []


class Application(Frame):
    """界面主程序"""

    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.searcher = TableSearcher()
        self.search_thread = None
        self.build_ui()
        self.configure_ui()

    def configure_ui(self):
        """界面适配"""
        if sys.platform == 'win32':
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)
        self.master.tk.call('tk', 'scaling', 1.5)

    def build_ui(self):
        """构建界面"""
        self.master.title("智能表格检索系统 v5.0")
        self.master.geometry("1280x720")

        # 控制面板
        control_frame = ttk.Frame(self.master)
        control_frame.pack(pady=10, fill=X, padx=10)

        ttk.Label(control_frame, text="数据目录:").grid(row=0, column=0, padx=5)
        self.dir_entry = ttk.Entry(control_frame, width=60)
        self.dir_entry.grid(row=0, column=1, padx=5)
        ttk.Button(control_frame, text="浏览", command=self.browse_dir).grid(row=0, column=2)

        ttk.Label(control_frame, text="搜索关键词:").grid(row=1, column=0, padx=5)
        self.keyword_entry = ttk.Entry(control_frame, width=40)
        self.keyword_entry.grid(row=1, column=1, padx=5)

        btn_frame = ttk.Frame(control_frame)
        btn_frame.grid(row=1, column=2, columnspan=2)
        ttk.Button(btn_frame, text="开始搜索", command=self.start_search).pack(side=LEFT, padx=2)
        ttk.Button(btn_frame, text="停止搜索", command=self.stop_search).pack(side=LEFT, padx=2)
        ttk.Button(btn_frame, text="导出结果", command=self.export_results).pack(side=LEFT, padx=2)

        # 结果展示
        result_frame = ttk.Frame(self.master)
        result_frame.pack(fill=BOTH, expand=True, padx=10, pady=5)

        columns = ("文件名", "匹配内容", "相似度", "匹配模式")
        self.result_tree = ttk.Treeview(result_frame, columns=columns, show='headings', height=20)

        # 列配置
        col_widths = [200, 600, 100, 200]
        for col, width in zip(columns, col_widths):
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=width, anchor='w')

        # 滚动条
        vsb = ttk.Scrollbar(result_frame, orient="vertical", command=self.result_tree.yview)
        hsb = ttk.Scrollbar(result_frame, orient="horizontal", command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        # 布局
        self.result_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # 状态栏
        self.status_var = StringVar()
        status_bar = ttk.Label(self.master, textvariable=self.status_var, relief="sunken")
        status_bar.pack(side=BOTTOM, fill=X)

        # 进度条
        self.progress = ttk.Progressbar(self.master, orient="horizontal", mode="determinate")
        self.progress.pack(fill=X, padx=10, pady=5)

    def update_status(self, message):
        self.master.after(0, lambda: self.status_var.set(message))

    def browse_dir(self):
        path = filedialog.askdirectory()
        if path:
            self.dir_entry.delete(0, END)
            self.dir_entry.insert(0, path)
            self.update_status(f"已选择目录: {path}")

    def start_search(self):
        if self.search_thread and self.search_thread.is_alive():
            return messagebox.showwarning("警告", "当前有搜索任务正在进行")

        keyword = self.keyword_entry.get().strip()
        search_dir = self.dir_entry.get().strip()

        if not keyword or not search_dir:
            return messagebox.showwarning("输入错误", "必须填写搜索关键词和目录")

        self.result_tree.delete(*self.result_tree.get_children())
        self.progress['value'] = 0

        # 获取文件列表
        file_list = []
        for ext in ['.csv', '.xls', '.xlsx', '.et', '.ods']:
            file_list.extend(Path(search_dir).rglob(f'*{ext}'))
        self.progress['maximum'] = len(file_list)

        # 启动线程
        self.search_thread = threading.Thread(
            target=self.run_search,
            args=(file_list, keyword),
            daemon=True
        )
        self.search_thread.start()

    def run_search(self, file_list, keyword):
        total_results = 0
        try:
            for idx, file_path in enumerate(file_list, 1):
                results = self.searcher.search_file(
                    file_path,
                    keyword,
                    lambda x: self.progress.step(1)
                )
                self.display_results(results)
                total_results += len(results)
                self.update_status(f"已处理 {idx}/{len(file_list)} 文件，找到 {total_results} 条结果")
            messagebox.showinfo("完成", f"成功处理 {len(file_list)} 个文件")
        except Exception as e:
            messagebox.showerror("错误", f"搜索异常终止: {str(e)}")
            logging.error(f"搜索异常: {str(e)}")

    def display_results(self, results):
        for res in results:
            try:
                content = ', '.join(f"{k}:{v}" for k, v in res['content'].items() if k != '_search_text')
                self.result_tree.insert('', 'end', values=(
                    res['file'],
                    content[:300] + '...' if len(content) > 300 else content,
                    res['similarity'],
                    res['pattern']
                ))
            except Exception as e:
                logging.error(f"结果显示错误: {str(e)}")

    def stop_search(self):
        if self.search_thread and self.search_thread.is_alive():
            self.search_thread.join(timeout=1)
            self.update_status("搜索已终止")

    def export_results(self):
        if not self.result_tree.get_children():
            return messagebox.showwarning("警告", "没有可导出的结果")

        save_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel文件", "*.xlsx"), ("CSV文件", "*.csv"), ("JSON文件", "*.json")]
        )
        if not save_path:
            return

        try:
            data = []
            for item in self.result_tree.get_children():
                values = self.result_tree.item(item)['values']
                data.append({
                    "文件名": values[0],
                    "匹配内容": values[1],
                    "相似度": values[2],
                    "匹配模式": values[3]
                })

            if save_path.endswith('.csv'):
                pd.DataFrame(data).to_csv(save_path, index=False, encoding='utf_8_sig')
            elif save_path.endswith('.json'):
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                pd.DataFrame(data).to_excel(save_path, index=False)
            messagebox.showinfo("成功", f"已导出 {len(data)} 条结果")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")


if __name__ == "__main__":
    root = Tk()
    app = Application(master=root)
    root.mainloop()