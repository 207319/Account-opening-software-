"""
智能表格检索系统 v4.1
更新内容：
1. 全面支持GB2312/GBK/GB18030编码
2. 增强编码检测容错机制
3. 优化中文特殊字符处理
4. 修复多线程进度更新问题
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
GB_ENCODINGS = ['gb2312', 'gbk', 'gb18030', 'ansi']  # 新增GB系列编码支持


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
        """智能检测文件编码（增强GB系列支持）"""
        try:
            with open(file_path, 'rb') as f:
                rawdata = f.read(100000)  # 增大采样量提高检测精度
                result = chardet.detect(rawdata)

                # 优先处理中文编码
                if result['encoding'] in ['GB2312', 'GBK', 'GB18030']:
                    return 'gb18030'  # 统一用gb18030处理
                elif result['encoding'] == 'ISO-8859-1':
                    return self.try_gb_encodings(file_path)
                return result['encoding'] or 'utf-8'
        except Exception as e:
            return 'utf-8'

    def try_gb_encodings(self, file_path):
        """尝试GB系列编码"""
        for encoding in GB_ENCODINGS:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    f.read(1024)
                    return encoding
            except:
                continue
        return 'gb18030'  # 默认回退

    def read_table(self, file_path):
        """增强版表格读取方法"""
        ext = file_path.suffix.lower()
        try:
            if ext == '.csv':
                return self.read_csv(file_path)
            elif ext in ('.xls', '.xlsx', '.et', '.ods'):
                return pd.read_excel(file_path, engine='openpyxl')
            else:
                return None
        except Exception as e:
            messagebox.showerror("错误", f"文件读取失败: {file_path}\n{str(e)}")
            return None

    def read_csv(self, file_path):
        """安全读取CSV文件"""
        encoding = self.detect_encoding(file_path)
        try:
            return pd.read_csv(file_path, encoding=encoding, engine='python', on_bad_lines='skip')
        except UnicodeDecodeError:
            # 回退机制尝试
            for enc in GB_ENCODINGS + ['utf-8']:
                try:
                    return pd.read_csv(file_path, encoding=enc, engine='python', on_bad_lines='skip')
                except:
                    continue
            raise

    def generate_variants(self, keyword):
        """生成搜索关键词变体（增强中文处理）"""
        variants = set()
        # 原始词及标准化
        variants.add(keyword.strip().lower())
        variants.add(keyword.strip().upper())

        # 拼音变体
        try:
            variants.add(''.join(lazy_pinyin(keyword, style=Style.NORMAL)))
            variants.add(''.join(lazy_pinyin(keyword, style=Style.FIRST_LETTER)))
        except Exception as e:
            messagebox.showwarning("拼音转换错误", f"关键词 '{keyword}' 拼音转换失败: {str(e)}")

        # 同音字变体
        for i, char in enumerate(keyword):
            if char in self.homophone_map:
                for homophone in self.homophone_map[char]:
                    new_word = keyword[:i] + homophone + keyword[i + 1:]
                    variants.add(new_word)
        return variants

    def search_file(self, file_path, keyword, progress_callback):
        """增强版文件搜索"""
        try:
            df = self.read_table(file_path)
            if df is None:
                return []

            results = []
            variants = self.generate_variants(keyword)

            # 空值处理
            df = df.fillna('').astype(str)

            for _, row in df.iterrows():
                row_str = '|'.join(row.values).lower()
                for variant in variants:
                    try:
                        similarity = jaro.jaro_winkler_metric(variant.lower(), row_str)
                        if similarity > 0.7:
                            results.append({
                                '文件': file_path.name,
                                '内容': row.to_dict(),
                                '相似度': f"{similarity:.0%}",
                                '匹配模式': variant
                            })
                    except Exception as e:
                        print(f"匹配计算错误: {str(e)}")
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
        self.total_files = 0
        self.processed_files = 0
        self.build_ui()

    def build_ui(self):
        """构建界面（优化布局）"""
        self.master.title("智能表格检索系统 v4.1")
        self.master.geometry("1000x680")

        # 顶部控制栏
        control_frame = Frame(self.master)
        control_frame.pack(pady=10, fill=X, padx=10)

        # 目录选择
        dir_frame = Frame(control_frame)
        dir_frame.pack(fill=X, pady=5)
        Label(dir_frame, text="数据目录:").pack(side=LEFT)
        self.dir_entry = Entry(dir_frame, width=60)
        self.dir_entry.pack(side=LEFT, padx=5)
        Button(dir_frame, text="浏览", command=self.browse_dir).pack(side=LEFT)

        # 搜索参数
        search_frame = Frame(control_frame)
        search_frame.pack(fill=X, pady=5)
        Label(search_frame, text="搜索关键词:").pack(side=LEFT)
        self.keyword_entry = Entry(search_frame, width=40)
        self.keyword_entry.pack(side=LEFT, padx=5)
        Button(search_frame, text="开始搜索", command=self.start_search).pack(side=LEFT, padx=5)
        Button(search_frame, text="导出结果", command=self.export_results).pack(side=LEFT)

        # 结果展示区
        result_frame = Frame(self.master)
        result_frame.pack(fill=BOTH, expand=True, padx=10, pady=5)

        columns = ("文件名", "匹配内容", "相似度", "匹配模式")
        self.result_tree = ttk.Treeview(
            result_frame,
            columns=columns,
            show='headings',
            selectmode='extended',
            height=20
        )

        # 列配置
        col_widths = [150, 400, 80, 150]
        for col, width in zip(columns, col_widths):
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=width, anchor='w')

        # 滚动条
        scroll_y = ttk.Scrollbar(result_frame, orient=VERTICAL, command=self.result_tree.yview)
        scroll_x = ttk.Scrollbar(result_frame, orient=HORIZONTAL, command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        self.result_tree.grid(row=0, column=0, sticky='nsew')
        scroll_y.grid(row=0, column=1, sticky='ns')
        scroll_x.grid(row=1, column=0, sticky='ew')

        # 进度条
        self.progress = ttk.Progressbar(
            self.master,
            orient=HORIZONTAL,
            mode='determinate',
            length=800
        )
        self.progress.pack(pady=10)

        # 状态栏
        self.status_var = StringVar()
        status_bar = Label(
            self.master,
            textvariable=self.status_var,
            bd=1, relief=SUNKEN, anchor=W
        )
        status_bar.pack(side=BOTTOM, fill=X)

    def update_status(self, message):
        """更新状态栏"""
        self.status_var.set(message)
        self.master.update()

    def browse_dir(self):
        """选择目录"""
        path = filedialog.askdirectory()
        if path:
            self.dir_entry.delete(0, END)
            self.dir_entry.insert(0, path)
            self.update_status(f"已选择目录: {path}")

    def start_search(self):
        """启动搜索"""
        if self.search_thread and self.search_thread.is_alive():
            messagebox.showwarning("警告", "当前有搜索任务正在进行")
            return

        keyword = self.keyword_entry.get().strip()
        search_dir = self.dir_entry.get().strip()

        if not keyword or not search_dir:
            messagebox.showwarning("输入错误", "必须填写搜索关键词和目录")
            return

        if not Path(search_dir).exists():
            messagebox.showerror("错误", "指定目录不存在")
            return

        # 重置状态
        self.result_tree.delete(*self.result_tree.get_children())
        self.progress['value'] = 0
        self.processed_files = 0

        # 获取文件总数
        self.total_files = sum(
            1 for ext in SUPPORTED_EXTS
            for _ in Path(search_dir).rglob(f'*{ext}')
        )
        self.progress['maximum'] = self.total_files

        # 启动线程
        self.search_thread = threading.Thread(
            target=self.run_search,
            args=(search_dir, keyword),
            daemon=True
        )
        self.search_thread.start()
        self.update_status("搜索已启动...")

    def run_search(self, search_dir, keyword):
        """执行搜索任务"""
        try:
            file_list = []
            for ext in SUPPORTED_EXTS:
                file_list.extend(Path(search_dir).rglob(f'*{ext}'))

            total_results = 0
            for file_path in file_list:
                results = self.searcher.search_file(
                    file_path,
                    keyword,
                    lambda x: self.progress.step(1)
                )
                self.display_results(results)
                total_results += len(results)
                self.processed_files += 1
                self.update_status(
                    f"已处理 {self.processed_files}/{self.total_files} 文件，找到 {total_results} 条结果"
                )

            messagebox.showinfo("完成",
                                f"搜索完成\n扫描文件: {self.total_files}\n找到结果: {total_results}")
        except Exception as e:
            messagebox.showerror("错误", f"搜索失败: {str(e)}")
        finally:
            self.update_status("就绪")

    def display_results(self, results):
        """显示搜索结果"""
        for res in results:
            self.result_tree.insert('', 'end', values=(
                res['文件'],
                ', '.join(f"{k}:{v}" for k, v in res['内容'].items()),
                res['相似度'],
                res['匹配模式']
            ))

    def export_results(self):
        """导出结果（增强异常处理）"""
        if not self.result_tree.get_children():
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        save_path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[
                ("Excel文件", "*.xlsx"),
                ("CSV文件", "*.csv"),
                ("JSON文件", "*.json")
            ]
        )
        if not save_path:
            return

        try:
            data = []
            for item in self.result_tree.get_children():
                values = self.result_tree.item(item)['values']
                data.append({
                    '文件名': values[0],
                    '内容': values[1],
                    '相似度': values[2],
                    '匹配模式': values[3]
                })

            df = pd.DataFrame(data)
            if save_path.endswith('.csv'):
                df.to_csv(save_path, index=False, encoding='gb18030')  # 中文编码兼容
            elif save_path.endswith('.json'):
                df.to_json(save_path, force_ascii=False, indent=2)
            else:
                df.to_excel(save_path, index=False)

            messagebox.showinfo("成功", f"成功导出 {len(data)} 条结果到: {save_path}")
        except PermissionError:
            messagebox.showerror("错误", "文件被占用，请关闭文件后重试")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")


if __name__ == "__main__":
    root = Tk()
    app = Application(master=root)
    # 设置DPI感知（Windows高分辨率优化）
    if sys.platform == 'win32':
        from ctypes import windll

        windll.shcore.SetProcessDpiAwareness(1)
    root.mainloop()