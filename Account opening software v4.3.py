# table_search.py
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

# 配置日志系统
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class UnicodeSafeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except:
            return str(obj)


class TableSearcher:
    def __init__(self):
        self.homophone_map = self.load_homophones()
        self.logger = logging.getLogger('Searcher')
        self.cache = {}  # 新增缓存系统

    def load_homophones(self):
        """加载同音字映射表"""
        default_map = {
            "楠": ["南", "男"],
            "陈": ["晨", "尘"],
            "张": ["章", "彰"],
            "李": ["里", "理"]
        }
        try:
            with open('homophones.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.warning(f"同音字文件加载失败，使用默认配置: {str(e)}")
            return default_map

    def detect_encoding(self, file_path):
        """增强型编码检测"""
        try:
            # 检测BOM头
            with open(file_path, 'rb') as f:
                bom = f.read(4)
                if bom.startswith(b'\xef\xbb\xbf'):
                    return 'utf-8-sig'
                if bom.startswith(b'\xff\xfe'):
                    return 'utf-16'
                if bom.startswith(b'\xfe\xff'):
                    return 'utf-16-be'

            # 大样本检测
            with open(file_path, 'rb') as f:
                rawdata = f.read(100000)
                result = chardet.detect(rawdata)

                if result['confidence'] > 0.85:
                    encoding = result['encoding'].lower()
                    if 'gb' in encoding:
                        return 'gb18030'
                    return encoding

                return self._try_encodings(file_path)
        except Exception as e:
            self.logger.error(f"编码检测异常: {str(e)}")
            return 'utf-8'

    def _try_encodings(self, file_path):
        """编码回退验证"""
        encodings = ['utf-8-sig', 'gb18030', 'gbk', 'utf-16', 'big5']
        for enc in encodings:
            try:
                with open(file_path, 'r', encoding=enc) as f:
                    f.read(1024)
                    self.logger.info(f"成功验证编码: {enc}")
                    return enc
            except Exception as e:
                continue
        return 'utf-8'

    def read_table(self, file_path):
        """安全读取表格文件"""
        self.logger.info(f"开始处理文件: {file_path}")
        try:
            ext = file_path.suffix.lower()
            if ext == '.csv':
                return self.read_csv(file_path)
            elif ext in ('.xls', '.xlsx', '.et', '.ods'):
                return pd.read_excel(file_path, engine='openpyxl', dtype=str, na_filter=False)
            else:
                self.logger.warning(f"不支持的文件格式: {ext}")
                return pd.DataFrame()
        except Exception as e:
            self.logger.error(f"文件读取失败: {str(e)}")
            return pd.DataFrame()

    def read_csv(self, file_path):
        """增强型CSV读取"""
        encoding = self.detect_encoding(file_path)
        self.logger.info(f"最终使用编码: {encoding}")

        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                engine='python',
                on_bad_lines='warn',
                dtype=str,
                keep_default_na=False
            )
            self.logger.debug(f"成功读取CSV文件，行数: {len(df)}")
            return df.fillna('').astype(str)
        except pd.errors.ParserError as e:
            self.logger.warning(f"CSV解析错误，尝试宽松模式: {str(e)}")
            try:
                return pd.read_csv(
                    file_path,
                    encoding=encoding,
                    engine='python',
                    error_bad_lines=False,
                    dtype=str,
                    keep_default_na=False
                )
            except Exception as e:
                self.logger.error(f"二次读取失败: {str(e)}")
                return pd.DataFrame()

    def generate_variants(self, keyword):
        """生成搜索变体"""
        variants = set()
        try:
            # 原始词处理
            variants.update([keyword, keyword.lower(), keyword.upper()])

            # 拼音处理
            variants.add(''.join(lazy_pinyin(keyword, style=Style.NORMAL)))
            variants.add(''.join(lazy_pinyin(keyword, style=Style.FIRST_LETTER)))

            # 同音字替换
            for i, char in enumerate(keyword):
                for homophone in self.homophone_map.get(char, []):
                    variants.add(keyword[:i] + homophone + keyword[i + 1:])

            self.logger.debug(f"生成搜索变体: {variants}")
            return variants
        except Exception as e:
            self.logger.error(f"变体生成失败: {str(e)}")
            return set()

    def safe_similarity(self, str1, str2):
        """安全相似度计算"""
        try:
            str1 = str(str1).lower().strip()
            str2 = str(str2).lower().strip()
            return jaro.jaro_winkler_metric(str1, str2)
        except Exception as e:
            self.logger.warning(f"相似度计算失败: {str(e)}")
            return 0

    def search_file(self, file_path, keyword, progress_callback):
        """文件搜索核心逻辑"""
        try:
            # 缓存检查
            cache_key = f"{file_path}-{keyword}"
            if cache_key in self.cache:
                self.logger.debug(f"缓存命中: {cache_key}")
                return self.cache[cache_key]

            df = self.read_table(file_path)
            if df.empty:
                return []

            results = []
            variants = self.generate_variants(keyword)

            for _, row in df.iterrows():
                row_str = '|'.join(str(x) for x in row.values)
                for variant in variants:
                    similarity = self.safe_similarity(variant, row_str)
                    if similarity >= 0.7:
                        results.append({
                            'file': file_path.name,
                            'content': {k: str(v) for k, v in row.items()},
                            'similarity': f"{similarity:.0%}",
                            'pattern': variant
                        })

            # 更新缓存
            self.cache[cache_key] = results
            progress_callback(len(results))
            return results
        except Exception as e:
            self.logger.error(f"文件处理异常: {str(e)}")
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
        """高DPI适配"""
        if sys.platform == 'win32':
            from ctypes import windll
            windll.shcore.SetProcessDpiAwareness(1)
        self.master.tk.call('tk', 'scaling', 1.5)

    def build_ui(self):
        """主界面构建"""
        self.master.title("智能表格检索系统 v4.4")
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

        btn_frame = ttk.Frame(control_frame)
        btn_frame.grid(row=1, column=2, columnspan=2)
        ttk.Button(btn_frame, text="开始搜索", command=self.start_search).pack(side=LEFT, padx=2)
        ttk.Button(btn_frame, text="停止搜索", command=self.stop_search).pack(side=LEFT, padx=2)
        ttk.Button(btn_frame, text="导出结果", command=self.export_results).pack(side=LEFT, padx=2)

        # 结果展示区
        result_frame = ttk.Frame(self.master)
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
        col_config = [
            ("文件名", 200),
            ("匹配内容", 600),
            ("相似度", 100),
            ("匹配模式", 200)
        ]
        for col, width in col_config:
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
        status_bar = ttk.Label(
            self.master,
            textvariable=self.status_var,
            relief="sunken"
        )
        status_bar.pack(side=BOTTOM, fill=X)

        # 进度条
        self.progress = ttk.Progressbar(
            self.master,
            orient="horizontal",
            mode="determinate"
        )
        self.progress.pack(fill=X, padx=10, pady=5)

    def update_status(self, message):
        """线程安全更新状态"""
        self.master.after(0, lambda: self.status_var.set(message))

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
                self.update_status(f"进度: {idx}/{len(file_list)} | 找到 {total_results} 条结果")
            messagebox.showinfo("完成", f"成功处理 {len(file_list)} 个文件，找到 {total_results} 条结果")
        except Exception as e:
            logging.error(f"搜索异常: {str(e)}")
            messagebox.showerror("错误", f"搜索异常终止: {str(e)}")

    def display_results(self, results):
        """安全更新结果"""
        for res in results:
            try:
                content = ', '.join(f"{k}:{v}" for k, v in res['content'].items())
                self.result_tree.insert('', 'end', values=(
                    res['file'],
                    content[:300] + '...' if len(content) > 300 else content,
                    res['similarity'],
                    res['pattern']
                ))
            except Exception as e:
                logging.error(f"结果显示错误: {str(e)}")

    def stop_search(self):
        """停止搜索"""
        if self.search_thread and self.search_thread.is_alive():
            self.search_thread.join(timeout=1)
            self.update_status("搜索已终止")

    def export_results(self):
        """导出结果"""
        if not self.result_tree.get_children():
            messagebox.showwarning("警告", "没有可导出的结果")
            return

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

            df = pd.DataFrame(data)
            if save_path.endswith('.csv'):
                df.to_csv(save_path, index=False, encoding='utf_8_sig')
            elif save_path.endswith('.json'):
                with open(save_path, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2, cls=UnicodeSafeEncoder)
            else:
                df.to_excel(save_path, index=False)
            messagebox.showinfo("成功", f"已导出 {len(data)} 条结果到: {save_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {str(e)}")

if __name__ == "__main__":
    root = Tk()
    app = Application(master=root)
    root.mainloop()