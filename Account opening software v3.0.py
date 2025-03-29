"""拼音模糊匹配（huanghaonan ↔ hhn）
同音字替换功能（黄浩南 ↔ 黄浩楠）
分布式处理支持（用于海量数据）
图形界面版本（使用PySimpleGUI）"""
import os
import sys  # 新增sys模块导入
import pandas as pd
import dask.dataframe as dd
from pathlib import Path
from tqdm import tqdm
import jaro
from pypinyin import lazy_pinyin, Style
import PySimpleGUI as sg  # 修正为正确的大小写
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import json

# ----------------------
# 核心处理模块
# ----------------------
class AdvancedTableSearcher:
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.results = []
        self.supported_formats = ['.csv', '.xls', '.xlsx', '.et']
        self.homophone_map = self._load_homophone_map()
        self.field_weights = {
            '姓名': 1.0, 'name': 1.0,
            '用户名': 0.9, 'user': 0.9,
            '工号': 0.8, '员工编号': 0.8
        }


    def _load_homophone_map(self):
        """加载同音字映射表"""
        try:
            with open('homophones.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {'楠': ['南', '男'], '南': ['楠', '男']}  # 示例数据

    def _generate_pinyin_variants(self, text):
        """生成拼音变体"""
        # 全拼
        full_pinyin = ''.join(lazy_pinyin(text, style=Style.NORMAL))
        # 首字母
        initials = ''.join(lazy_pinyin(text, style=Style.FIRST_LETTER))
        return {text, full_pinyin, initials}

    def _generate_homophones(self, text):
        """生成同音字组合"""
        variants = set()
        for char in text:
            if char in self.homophone_map:
                for homophone in self.homophone_map[char]:
                    variants.add(text.replace(char, homophone))
        return variants

    def _get_search_patterns(self, keyword):
        """生成搜索模式集合"""
        patterns = set()
        # 原始关键词
        patterns.add(keyword)
        # 拼音变体
        patterns.update(self._generate_pinyin_variants(keyword))
        # 同音字变体
        patterns.update(self._generate_homophones(keyword))
        return patterns

    def _distributed_search(self, file_path, patterns, threshold):
        """分布式处理单个文件"""
        results = []
        try:
            if file_path.suffix == '.csv':
                df = dd.read_csv(file_path, dtype=str)
            else:
                df = dd.read_excel(file_path, dtype=str)

            for pattern in patterns:
                mask = df.map_partitions(
                    lambda d: d.astype(str).apply(
                        lambda row: any(
                            jaro.jaro_winkler_metric(pattern.lower(), str(cell).lower()) > threshold
                            for cell in row
                        ),
                        meta=('bool',)
                    )
                )
                matches = df[mask].compute()
                for _, row in matches.iterrows():
                    results.append({
                        'file': file_path.name,
                        'data': row.to_dict(),
                        'pattern': pattern
                    })
        except Exception as e:
            print(f"Error processing {file_path.name}: {str(e)}")
        return results

    def search(self, keyword, threshold=0.8, distributed=False):
        """执行综合搜索"""
        self.results.clear()
        patterns = self._get_search_patterns(keyword)

        if distributed:
            with ProcessPoolExecutor() as executor:
                futures = []
                files = [f for f in self.data_dir.glob('*.*') if f.suffix in self.supported_formats]
                for file in files:
                    futures.append(executor.submit(
                        self._distributed_search, file, patterns, threshold
                    ))

                for future in tqdm(futures, desc="分布式处理"):
                    self.results.extend(future.result())
        else:
            files = [f for f in self.data_dir.glob('*.*') if f.suffix in self.supported_formats]
            for file in tqdm(files, desc="本地处理"):
                if file.suffix == '.csv':
                    df = pd.read_csv(file, dtype=str)
                else:
                    df = pd.read_excel(file, dtype=str)

                for pattern in patterns:
                    mask = df.astype(str).apply(
                        lambda row: any(
                            jaro.jaro_winkler_metric(pattern.lower(), str(cell).lower()) > threshold
                            for cell in row
                        ),
                        axis=1
                    )
                    matches = df[mask]
                    for _, row in matches.iterrows():
                        self.results.append({
                            'file': file.name,
                            'data': row.to_dict(),
                            'pattern': pattern
                        })

        # 结果去重和排序
        df_results = pd.DataFrame(self.results)
        df_results = df_results.drop_duplicates(subset=['file', 'data'])
        df_results['score'] = df_results.apply(
            lambda x: jaro.jaro_winkler_metric(keyword, x['pattern']),
            axis=1
        )
        self.results = df_results.sort_values('score', ascending=False).to_dict('records')
        return self.results


# ----------------------
# 图形界面模块
# ----------------------
class TableSearchGUI:
    def __init__(self):
        self.layout = [
            [sg.T("数据目录："), sg.I(key='-DIR-'), sg.FolderBrowse()],
            [sg.T("搜索关键词："), sg.I(key='-KEYWORD-')],
            [sg.T("匹配阈值："), sg.Slider((0.1,1.0), 0.8, resolution=0.05, orientation='h', key='-THRESHOLD-')],
            [sg.Checkbox("分布式处理", key='-DIST-'), sg.Checkbox("启用拼音匹配", default=True, key='-PINYIN-')],
            [sg.B("开始搜索"), sg.B("导出结果"), sg.Exit()],
            [sg.ML(size=(80,20), key='-OUTPUT-', autoscroll=True)]
        ]
        self.window = sg.Window("智能表格检索系统 v3.0", self.layout)

    def run(self):
        while True:
            event, values = self.window.read()
            if event in (sg.WIN_CLOSED, 'Exit'):
                break

            if event == "开始搜索":
                self._handle_search(values)

            if event == "导出结果":
                self._handle_export()

        self.window.close()

    def _handle_search(self, values):
        keyword = values['-KEYWORD-']
        if not keyword:
            sg.popup_error("请输入搜索关键词！")
            return

        self.searcher = AdvancedTableSearcher(values['-DIR-'])
        try:
            results = self.searcher.search(
                keyword,
                threshold=values['-THRESHOLD-'],
                distributed=values['-DIST-']
            )
            output = "\n".join(
                f"{res['file']} | 匹配模式：{res['pattern']}\n{json.dumps(res['data'], ensure_ascii=False)}\n"
                for res in results[:10]  # 显示前10条
            )
            self.window['-OUTPUT-'].update(output)
        except Exception as e:
            sg.popup_error(f"搜索失败：{str(e)}")

    def _handle_export(self):
        if not self.searcher or not self.searcher.results:
            sg.popup_error("没有可导出的结果！")
            return

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = sg.popup_get_file("保存文件",
                                         save_as=True,
                                         default_extension=".xlsx",
                                         file_types=(("Excel Files", "*.xlsx"), ("CSV Files", "*.csv")))

            if filename:
                df = pd.DataFrame(self.searcher.results)
                if filename.endswith('.csv'):
                    df.to_csv(filename, index=False, encoding='utf_8_sig')
                else:
                    df.to_excel(filename, index=False)
                sg.popup(f"成功导出 {len(df)} 条记录到：{filename}")
        except Exception as e:
            sg.popup_error(f"导出失败：{str(e)}")


# ----------------------
# 运行入口
# ----------------------
if __name__ == "__main__":
    # 添加sys模块后，命令行参数检测可正常使用
    if len(sys.argv) > 1 and sys.argv[1] == 'cli':
        searcher = AdvancedTableSearcher("./data")
        results = searcher.search("黄浩楠", distributed=True)
        print(json.dumps(results, indent=2, ensure_ascii=False))
    else:
        gui = TableSearchGUI()
        gui.run()