"""
智能表格检索系统 v4.0
功能特性：
1. 永久免费开源（使用Tkinter替代PySimpleGUI）
2. 支持全格式表格文件（CSV/Excel/WPS/ODS）
3. 增强型中文检索（拼音+同音字）
4. 多线程加速处理
5. 智能编码检测
"""

import os
import sys
import json
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

# 常量定义
SUPPORTED_EXTS = ['.csv', '.xls', '.xlsx', '.et', '.ods']
HOMOPHONE_FILE = 'homophones.json'
DEFAULT_ENCODINGS = ['utf-8', 'gb18030', 'big5', 'shift_jis']


class TableSearcher:
    def __init__(self):
        self.homophone_map = self.load_homophones()

    def load_homophones(self):
        """加载同音字映射表"""
        try:
            with open(HOMOPHONE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            messagebox.showwarning("警告", f"同音字文件加载失败: {str(e)}")
            return {}

    def detect_encoding(self, file_path):
        """智能检测文件编码"""
        try:
            with open(file_path, 'rb') as f:
                rawdata = f.read(10000)
                result = chardet.detect(rawdata)
                return result['encoding'] or 'utf-8'
        except Exception as e:
            return 'utf-8'

    def read_table(self, file_path):
        """通用表格读取方法"""
        ext = file_path.suffix.lower()
        try:
            if ext == '.csv':
                encoding = self.detect_encoding(file_path)
                return pd.read_csv(file_path, encoding=encoding, engine='python')
            elif ext in ('.xls', '.xlsx', '.et', '.ods'):
                return pd.read_excel(file_path, engine='openpyxl')
            else:
                return None
        except Exception as e:
            messagebox.showerror("错误", f"文件读取失败: {file_path}\n{str(e)}")
            return None

    def generate_variants(self, keyword):
        """生成搜索关键词变体"""
        variants = set()
        # 原始词
        variants.add(keyword)
        # 拼音变体
        variants.add(''.join(lazy_pinyin(keyword, style=Style.NORMAL)))
        variants.add(''.join(lazy_pinyin(keyword, style=Style.FIRST_LETTER)))
        # 同音字变体
        for i, char in enumerate(keyword):
            if char in self.homophone_map:
                for homophone in self.homophone_map[char]:
                    new_word = keyword[:i] + homophone + keyword[i + 1:]
                    variants.add(new_word)
        return variants

    def search_file(self, file_path, keyword, progress_callback):
        """单个文件搜索"""
        try:
            df = self.read_table(file_path)
            if df is None:
                return []

            results = []
            variants = self.generate_variants(keyword)

            for _, row in df.iterrows():
                row_str = '|'.join([str(x) for x in row.values])
                for variant in variants:
                    similarity = jaro.jaro_winkler_metric(variant.lower(), row_str.lower())
                    if similarity > 0.7:
                        results.append({
                            '文件': file_path.name,
                            '内容': row.to_dict(),
                            '相似度': f"{similarity:.0%}",
                            '匹配模式': variant
                        })
            progress_callback(len(results))
            return results
        except Exception as e:
            messagebox.showerror("错误", f"处理文件失败: {file_path}\n{str(e)}")
            return []


class Application(Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.searcher = TableSearcher()
        self.search_thread = None
        self.build_ui()

    def build_ui(self):
        """构建界面"""
        self.master.title("智能表格检索系统 v4.0")
        self.master.geometry("800x600")

        # 顶部控制栏
        control_frame = Frame(self.master)
        control_frame.pack(pady=10, fill=X)

        Label(control_frame, text="数据目录:").grid(row=0, column=0, padx=5)
        self.dir_entry = Entry(control_frame, width=40)
        self.dir_entry.grid(row=0, column=1, padx=5)
        Button(control_frame, text="浏览", command=self.browse_dir).grid(row=0, column=2, padx=5)

        Label(control_frame, text="搜索关键词:").grid(row=1, column=0, padx=5)
        self.keyword_entry = Entry(control_frame, width=40)
        self.keyword_entry.grid(row=1, column=1, padx=5)

        Button(control_frame, text="开始搜索", command=self.start_search).grid(row=1, column=2, padx=5)
        Button(control_frame, text="导出结果", command=self.export_results).grid(row=1, column=3, padx=5)

        # 结果展示区
        result_frame = Frame(self.master)
        result_frame.pack(fill=BOTH, expand=True, padx=10, pady=5)

        columns = ("文件名", "匹配内容", "相似度", "匹配模式")
        self.result_tree = ttk.Treeview(
            result_frame, columns=columns, show='headings', selectmode='browse'
        )

        for col in columns:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=150, anchor='w')

        scrollbar = ttk.Scrollbar(result_frame, orient=VERTICAL, command=self.result_tree.yview)
        self.result_tree.configure(yscrollcommand=scrollbar.set)

        self.result_tree.pack(side=LEFT, fill=BOTH, expand=True)
        scrollbar.pack(side=RIGHT, fill=Y)

        # 进度条
        self.progress = ttk.Progressbar(self.master, orient=HORIZONTAL, mode='determinate')
        self.progress.pack(fill=X, padx=10, pady=5)

    def browse_dir(self):
        """选择目录"""
        path = filedialog.askdirectory()
        if path:
            self.dir_entry.delete(0, END)
            self.dir_entry.insert(0, path)

    def start_search(self):
        """启动搜索线程"""
        if self.search_thread and self.search_thread.is_alive():
            messagebox.showwarning("警告", "已有搜索正在进行")
            return

        keyword = self.keyword_entry.get().strip()
        search_dir = self.dir_entry.get().strip()

        if not keyword or not search_dir:
            messagebox.showwarning("警告", "请输入搜索关键词和目录")
            return

        self.result_tree.delete(*self.result_tree.get_children())
        self.progress['value'] = 0

        self.search_thread = threading.Thread(
            target=self.run_search,
            args=(search_dir, keyword),
            daemon=True
        )
        self.search_thread.start()

    def run_search(self, search_dir, keyword):
        """执行搜索"""
        try:
            total_files = 0
            found_results = 0
            file_list = []

            for ext in SUPPORTED_EXTS:
                file_list.extend(Path(search_dir).rglob(f'*{ext}'))

            self.progress['maximum'] = len(file_list)

            for file_path in file_list:
                results = self.searcher.search_file(file_path, keyword, self.update_progress)
                found_results += len(results)
                self.display_results(results)
                total_files += 1

            messagebox.showinfo("完成",
                                f"搜索完成\n扫描文件: {total_files}\n找到结果: {found_results}")
        except Exception as e:
            messagebox.showerror("错误", f"搜索失败: {str(e)}")

    def update_progress(self, count):
        """更新进度"""
        self.progress['value'] += 1
        self.master.update_idletasks()

    def display_results(self, results):
        """显示结果"""
        for res in results:
            self.result_tree.insert('', 'end', values=(
                res['文件'],
                ', '.join(f"{k}:{v}" for k, v in res['内容'].items()),
                res['相似度'],
                res['匹配模式']
            ))

    def export_results(self):
        """导出结果"""
        items = self.result_tree.get_children()
        if not items:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        save_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel文件", "*.xlsx"), ("CSV文件", "*.csv")]
        )
        if not save_path:
            return

        try:
            data = []
            for item in items:
                values = self.result_tree.item(item)['values']
                data.append({
                    '文件名': values[0],
                    '内容': values[1],
                    '相似度': values[2],
                    '匹配模式': values[3]
                })

            df = pd.DataFrame(data)
            if save_path.endswith('.csv'):
                df.to_csv(save_path, index=False, encoding='utf_8_sig')
            else:
                df.to_excel(save_path, index=False)

            messagebox.showinfo("成功", f"成功导出 {len(data)} 条结果到: {save_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")


if __name__ == "__main__":
    root = Tk()
    app = Application(master=root)
    app.mainloop()