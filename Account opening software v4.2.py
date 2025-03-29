"""
智能表格检索系统 v4.2
更新内容：
1. 完全兼容UTF-8全系列编码（含BOM头）
2. 优化编码检测优先级策略
3. 增强特殊字符处理能力
4. 修复多编码混合场景下的bug
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
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='app.log'
)

# 常量定义
SUPPORTED_EXTS = ['.csv', '.xls', '.xlsx', '.et', '.ods']
HOMOPHONE_FILE = 'homophones.json'
ENCODING_PRIORITY = ['utf-8-sig', 'utf_8', 'gb18030', 'gbk', 'gb2312', 'big5']  # 调整编码优先级


class UnicodeSafeEncoder(json.JSONEncoder):
    """处理特殊字符的JSON编码器"""

    def default(self, obj):
        return str(obj)


class TableSearcher:
    def __init__(self):
        self.homophone_map = self.load_homophones()
        self.logger = logging.getLogger('Searcher')

    def load_homophones(self):
        """加载同音字映射表（增强异常处理）"""
        try:
            with open(HOMOPHONE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"同音字文件加载失败: {str(e)}")
            return {}

    def detect_encoding(self, file_path):
        """智能编码检测（UTF-8优化）"""
        try:
            # 优先检测UTF-8 BOM
            with open(file_path, 'rb') as f:
                bom = f.read(3)
                if bom == b'\xef\xbb\xbf':
                    return 'utf-8-sig'

            # 使用大样本提高检测精度
            with open(file_path, 'rb') as f:
                rawdata = f.read(100000)
                result = chardet.detect(rawdata)

                # 置信度阈值
                if result['confidence'] > 0.9:
                    return result['encoding'].lower()

                # 中文编码优先
                if any(enc in result['encoding'].lower() for enc in ['gb', 'big5']):
                    return self._try_chinese_encodings(file_path)

                return 'utf-8'
        except Exception as e:
            self.logger.error(f"编码检测失败: {str(e)}")
            return 'utf-8'

    def _try_chinese_encodings(self, file_path):
        """中文编码专项检测"""
        for enc in ['gb18030', 'gbk', 'gb2312', 'big5']:
            try:
                with open(file_path, 'r', encoding=enc) as f:
                    f.read(1024)
                    return enc
            except:
                continue
        return 'gb18030'

    def read_table(self, file_path):
        """通用表格读取（UTF-8优化）"""
        try:
            ext = file_path.suffix.lower()
            if ext == '.csv':
                return self.read_csv(file_path)
            elif ext in ('.xls', '.xlsx', '.et', '.ods'):
                return pd.read_excel(file_path, engine='openpyxl', dtype=str)
            return None
        except Exception as e:
            self.logger.error(f"文件读取失败: {file_path} - {str(e)}")
            return None

    def read_csv(self, file_path):
        """安全读取CSV文件（多层编码回退）"""
        for encoding in ENCODING_PRIORITY:
            try:
                df = pd.read_csv(
                    file_path,
                    encoding=encoding,
                    engine='python',
                    on_bad_lines='warn',
                    dtype=str,
                    keep_default_na=False
                )
                return df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                self.logger.warning(f"尝试编码 {encoding} 失败: {str(e)}")
        return pd.DataFrame()

    def generate_variants(self, keyword):
        """生成搜索变体（增强特殊字符支持）"""
        variants = set()
        try:
            # 标准化处理
            variants.add(keyword.strip())
            variants.add(keyword.lower())
            variants.add(keyword.upper())

            # 拼音处理
            variants.update([
                ''.join(lazy_pinyin(keyword, style=Style.NORMAL)),
                ''.join(lazy_pinyin(keyword, style=Style.FIRST_LETTER)),
                ''.join(lazy_pinyin(keyword, style=Style.TONE2)),
            ])

            # 同音字替换
            for i, char in enumerate(keyword):
                for homophone in self.homophone_map.get(char, []):
                    variants.add(keyword[:i] + homophone + keyword[i + 1:])
        except Exception as e:
            self.logger.error(f"变体生成失败: {str(e)}")
        return variants

    def safe_similarity(self, str1, str2):
        """安全相似度计算"""
        try:
            return jaro.jaro_winkler_metric(str1.lower(), str2.lower())
        except:
            return 0

    def search_file(self, file_path, keyword, progress_callback):
        """文件搜索核心逻辑"""
        try:
            df = self.read_table(file_path)
            if df.empty:
                return []

            results = []
            variants = self.generate_variants(keyword)

            for _, row in df.iterrows():
                row_str = '|'.join([str(x) for x in row.values])
                for variant in variants:
                    similarity = self.safe_similarity(variant, row_str)
                    if similarity > 0.7:
                        results.append({
                            'file': file_path.name,
                            'content': {k: str(v) for k, v in row.items()},
                            'similarity': f"{similarity:.0%}",
                            'pattern': variant,
                            'encoding': df.encoding  # 记录实际使用编码
                        })
            progress_callback(len(results))
            return results
        except Exception as e:
            self.logger.error(f"文件处理失败: {file_path} - {str(e)}")
            return []


class Application(Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.searcher = TableSearcher()
        self.search_thread = None
        self.build_ui()
        self.configure_ui()

    def configure_ui(self):
        """界面配置（高DPI优化）"""
        if sys.platform == 'win32':
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)
        self.master.tk.call('tk', 'scaling', 1.5)

    def build_ui(self):
        """构建现代风格界面"""
        self.master.title("智能表格检索系统 v4.2")
        self.master.geometry("1200x800")

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

        ttk.Button(control_frame, text="开始搜索", command=self.start_search).grid(row=1, column=2, padx=5)
        ttk.Button(control_frame, text="导出结果", command=self.export_results).grid(row=1, column=3)
        ttk.Button(control_frame, text="日志查看", command=self.view_logs).grid(row=1, column=4)

        # 结果展示
        result_frame = ttk.Frame(self.master)
        result_frame.pack(fill=BOTH, expand=True, padx=10, pady=5)

        columns = ("文件名", "文件编码", "匹配内容", "相似度", "匹配模式")
        self.result_tree = ttk.Treeview(
            result_frame,
            columns=columns,
            show='headings',
            selectmode='extended',
            height=15
        )

        # 配置列
        col_config = [
            ("文件名", 200),
            ("文件编码", 100),
            ("匹配内容", 500),
            ("相似度", 80),
            ("匹配模式", 150)
        ]
        for col, width in col_config:
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=width, anchor='w')

        # 滚动条
        scroll_y = ttk.Scrollbar(result_frame, orient=VERTICAL, command=self.result_tree.yview)
        scroll_x = ttk.Scrollbar(result_frame, orient=HORIZONTAL, command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=scroll_y.set, xscrollcommand=scroll_x.set)

        # 布局
        self.result_tree.grid(row=0, column=0, sticky='nsew')
        scroll_y.grid(row=0, column=1, sticky='ns')
        scroll_x.grid(row=1, column=0, sticky='ew')

        # 状态栏
        self.status_var = StringVar()
        status_bar = ttk.Label(
            self.master,
            textvariable=self.status_var,
            relief=SUNKEN
        )
        status_bar.pack(side=BOTTOM, fill=X)

        # 进度条
        self.progress = ttk.Progressbar(
            self.master,
            orient=HORIZONTAL,
            mode='determinate'
        )
        self.progress.pack(fill=X, padx=10, pady=5)

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
        """启动搜索任务"""
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

        # 准备搜索
        self.result_tree.delete(*self.result_tree.get_children())
        self.progress['value'] = 0
        self.update_status("正在初始化搜索...")

        # 获取文件列表
        file_list = []
        for ext in SUPPORTED_EXTS:
            file_list.extend(Path(search_dir).rglob(f'*{ext}'))
        total_files = len(file_list)
        self.progress['maximum'] = total_files

        # 启动线程
        self.search_thread = threading.Thread(
            target=self.run_search,
            args=(file_list, keyword),
            daemon=True
        )
        self.search_thread.start()

    def run_search(self, file_list, keyword):
        """执行搜索"""
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
                self.update_status(
                    f"进度: {idx}/{len(file_list)} 文件 | 找到 {total_results} 条结果 | 当前文件: {file_path.name}"
                )
            messagebox.showinfo("完成", f"成功处理 {len(file_list)} 个文件，找到 {total_results} 条结果")
        except Exception as e:
            messagebox.showerror("错误", f"搜索异常终止: {str(e)}")
        finally:
            self.update_status("就绪")

    def display_results(self, results):
        """显示结果"""
        for res in results:
            self.result_tree.insert('', 'end', values=(
                res['file'],
                res.get('encoding', '未知'),
                ', '.join(f"{k}:{v}" for k, v in res['content'].items()),
                res['similarity'],
                res['pattern']
            ))

    def export_results(self):
        """导出结果"""
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
                    "文件名": values[0],
                    "文件编码": values[1],
                    "匹配内容": values[2],
                    "相似度": values[3],
                    "匹配模式": values[4]
                })

            if save_path.endswith('.json'):
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, cls=UnicodeSafeEncoder)
            else:
                df = pd.DataFrame(data)
                if save_path.endswith('.csv'):
                    df.to_csv(save_path, index=False, encoding='utf_8_sig')
                else:
                    df.to_excel(save_path, index=False)

            messagebox.showinfo("成功", f"已导出 {len(data)} 条结果到: {save_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")

    def view_logs(self):
        """查看日志"""
        try:
            if sys.platform == 'win32':
                os.startfile('app.log')
            else:
                os.system('open app.log')
        except:
            messagebox.showerror("错误", "无法打开日志文件")


if __name__ == "__main__":
    root = Tk()
    app = Application(master=root)
    root.mainloop()