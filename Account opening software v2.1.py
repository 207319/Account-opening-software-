'''智能阈值系统：
自动根据关键词长度调整基准阈值
支持手动覆盖阈值设置
混合匹配算法：
字段权重系统：
增强结果展示：
智能结果导出：
异常处理机制
自动跳过无法解码的文件
错误文件会有醒目的⚠️标识
结果导出自动创建目录
自动处理空值和异常数据格式'''

import os
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import jaro
from datetime import datetime


class SmartTableSearcher:
    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.results = []
        self.supported_formats = ['.csv', '.xls', '.xlsx', '.et']
        self.field_weights = {
            '姓名': 1.0, 'name': 1.0,
            '用户名': 0.9, 'user': 0.9,
            '工号': 0.8, '员工编号': 0.8,
            '电话': 0.7, '手机': 0.7,
            '部门': 0.6, '职务': 0.6
        }

    def _auto_adjust_threshold(self, keyword):
        """智能阈值调节系统"""
        length = len(keyword)
        if length <= 2:
            return 0.95
        elif 3 <= length <= 5:
            return 0.85
        else:
            return max(0.7, 1.0 - (length * 0.03))

    def search_in_tables(self, keyword, custom_threshold=None):
        """执行智能搜索"""
        auto_threshold = self._auto_adjust_threshold(keyword)
        final_threshold = custom_threshold or auto_threshold

        self.results.clear()
        file_list = [f for f in self.data_dir.glob('*.*') if f.suffix.lower() in self.supported_formats]

        with tqdm(total=len(file_list), desc="🚀 智能扫描进度") as pbar:
            for file_path in file_list:
                try:
                    if file_path.suffix.lower() == '.csv':
                        self._process_csv(file_path, keyword, final_threshold)
                    else:
                        self._process_excel(file_path, keyword, final_threshold)
                except Exception as e:
                    print(f"\n⚠️ 文件读取异常: {file_path.name} - {str(e)}")
                finally:
                    pbar.update(1)

        # 智能结果排序
        # 修正后的智能结果排序部分
        self.results.sort(
            key=lambda x: -float(x['similarity'].strip('%')),
            reverse=False
        )
        return self.results

    def _process_csv(self, file_path, keyword, threshold):
        """处理CSV文件（自动检测编码）"""
        encodings = ['utf-8', 'gbk', 'gb18030', 'big5']
        for enc in encodings:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding=enc)
                df.fillna('', inplace=True)
                self._analyze_dataframe(df, keyword, file_path.name, "CSV", threshold)
                return
            except (UnicodeDecodeError, pd.errors.ParserError):
                continue
        print(f"\n❌ 无法解码文件: {file_path.name}")

    def _process_excel(self, file_path, keyword, threshold):
        """处理Excel/WPS文件"""
        try:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name, dtype=str)
                df.fillna('', inplace=True)
                self._analyze_dataframe(df, keyword, file_path.name, sheet_name, threshold)
        except Exception as e:
            print(f"\n❌ Excel读取失败: {str(e)}")

    def _analyze_dataframe(self, df, keyword, filename, sheetname, threshold):
        """执行智能字段分析"""
        for _, row in df.iterrows():
            best_match = {
                'similarity': 0,
                'field': None,
                'value': ''
            }

            for col_name, cell_value in row.items():
                clean_value = str(cell_value).strip()
                if not clean_value:
                    continue

                # 使用Jaro-Winkler算法计算相似度
                similarity = jaro.jaro_winkler_metric(
                    keyword.lower(),
                    clean_value.lower()
                )

                # 应用字段权重加成
                weight = self.field_weights.get(col_name.strip(), 0.5)
                similarity *= (1 + weight * 0.2)

                if similarity > best_match['similarity']:
                    best_match.update({
                        'similarity': similarity,
                        'field': col_name,
                        'value': clean_value
                    })

            if best_match['similarity'] >= threshold:
                self._record_match(
                    row, filename, sheetname,
                    best_match['similarity'], best_match['field']
                )

    def _record_match(self, row, filename, sheetname, similarity, match_field):
        """记录匹配结果"""
        record = {
            'file': filename,
            'sheet': sheetname,
            'similarity': f"{similarity:.1%}",
            'match_field': match_field,
            'data': {k: v for k, v in row.items() if pd.notna(v)},
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.results.append(record)

    def export_results(self, format_type='csv', output_dir='results'):
        """导出搜索结果"""
        if not self.results:
            print("🟡 没有可导出的结果")
            return

        df = pd.json_normalize(self.results, sep='|')
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"search_results_{timestamp}"

        try:
            if format_type == 'csv':
                full_path = output_path / f"{filename}.csv"
                df.to_csv(full_path, index=False, encoding='utf_8_sig')
            elif format_type == 'excel':
                full_path = output_path / f"{filename}.xlsx"
                df.to_excel(full_path, index=False)
            else:
                print("🔴 不支持的导出格式")
                return

            print(f"\n✅ 成功导出 {len(self.results)} 条结果到: {full_path}")
        except Exception as e:
            print(f"\n🔴 导出失败: {str(e)}")


def main():
    print("=" * 50)
    print("智能表格检索系统 v2.1")
    print("=" * 50)

    DATA_DIR = input("请输入数据目录路径（默认./data）: ").strip() or "./data"
    data_path = Path(DATA_DIR)

    if not data_path.exists():
        print(f"\n🔴 错误：目录不存在 {data_path}")
        return

    searcher = SmartTableSearcher(data_path)

    while True:
        keyword = input("\n🔍 请输入检索关键词（q退出）: ").strip()
        if keyword.lower() == 'q':
            break

        # 获取阈值设置
        auto_threshold = searcher._auto_adjust_threshold(keyword)
        threshold_input = input(
            f"📊 推荐阈值 {auto_threshold:.2f}（直接回车使用推荐值或输入自定义值 0.1-1.0）: "
        ).strip()

        try:
            threshold = float(threshold_input) if threshold_input else auto_threshold
            threshold = max(0.1, min(1.0, threshold))
        except ValueError:
            print("⚠️ 输入无效，使用推荐阈值")
            threshold = auto_threshold

        print(f"🔄 正在使用阈值 {threshold:.2f} 进行搜索...")
        results = searcher.search_in_tables(keyword, threshold)

        if not results:
            print("\n🔍 未找到匹配结果")
            continue

        # 显示前5条结果
        print(f"\n🎉 找到 {len(results)} 条相关记录（显示前5条）:")
        for idx, record in enumerate(results[:5], 1):
            print(f"\n【结果{idx}】")
            print(f"📁 文件: {record['file']}")
            print(f"📑 工作表: {record['sheet']}")
            print(f"📈 匹配度: {record['similarity']}")
            print(f"🔖 匹配字段: {record['match_field']}")
            print("🔍 详细信息:")
            for k, v in record['data'].items():
                print(f"  {k}: {v}")

        # 导出功能
        if input("\n💾 是否导出全部结果？(y/n): ").lower() == 'y':
            fmt = input("选择导出格式 (csv/excel): ").lower().strip()
            if fmt in ('csv', 'excel'):
                searcher.export_results(fmt)
            else:
                print("⚠️ 无效格式，取消导出")


if __name__ == "__main__":
    main()