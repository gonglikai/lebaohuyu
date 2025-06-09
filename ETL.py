import pandas as pd
import os
import time
import logging

# 配置全局日志，格式包含时间和消息内容
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCleaner:
    """
    数据清洗工具：
      - 标准化列名
      - 去重与空值处理
      - 事件类型与设备类型过滤
      - 时间戳解析并提取日期/小时
    """

    # 定义允许的事件类型和设备类型集合
    VALID_EVENT_TYPES = {'Login', 'Logout', 'LevelComplete', 'InAppPurchase', 'SocialInteraction'}
    VALID_DEVICES = {'Android', 'iOS', 'PC'}

    def __init__(self, src, dst, chunk_size=50000):
        """
        初始化清洗器
        :param src: 源 CSV 文件路径
        :param dst: 输出 CSV 文件路径
        :param chunk_size: 分块读取行数
        """
        self.src = src
        self.dst = dst
        self.chunk_size = chunk_size
        # 用于统计总行数和保留行数
        self.stats = {'total': 0, 'kept': 0}

    def _standardize(self, df):
        """
        标准化列名：将不同命名方式统一为小写下划线
        并确保必需列存在
        """
        mapping = {
            'EventID': 'event_id',
            'PlayerID': 'player_id',
            'EventTimestamp': 'event_timestamp',
            'EventType': 'event_type',
            'EventDetails': 'event_details',
            'DeviceType': 'device_type',
            'Location': 'location'
        }
        df = df.rename(columns=mapping)
        # 检查必需列是否齐全
        for col in ['event_id','player_id','event_timestamp','event_type']:
            if col not in df.columns:
                raise KeyError(f"缺少必需列: {col}")
        # 填充可能缺失的 event_details 列
        if 'event_details' not in df:
            df['event_details'] = ''
        return df

    def _clean(self, df):
        """
        对单块数据进行清洗：
          1. 标准化列名
          2. 去除完全重复行
          3. 删除关键字段空值行
          4. 过滤不合法的事件/设备类型
          5. 解析时间戳并生成日期、小时列
        """
        before = len(df)
        # 标准化并去重、删除缺失
        df = (
            df.pipe(self._standardize)
              .drop_duplicates()
              .dropna(subset=['event_id','player_id','event_timestamp','event_type'])
        )
        # 文本字段统一格式
        df['event_id'] = df['event_id'].str.upper().str.strip()
        df['player_id'] = df['player_id'].str.upper().str.strip()
        df['event_type'] = df['event_type'].str.strip()
        # device_type 可能不存在，则填充 Unknown
        df['device_type'] = df.get('device_type', 'Unknown').astype(str).str.strip()
        # 仅保留有效事件和设备
        df = df[df['event_type'].isin(self.VALID_EVENT_TYPES)]
        df = df[df['device_type'].isin(self.VALID_DEVICES)]
        # 解析时间戳，忽略无法解析的
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')
        df = df.dropna(subset=['event_timestamp'])
        # 提取日期和小时，用于后续分析
        df['event_date'] = df['event_timestamp'].dt.date
        df['event_hour'] = df['event_timestamp'].dt.hour

        kept = len(df)
        # 更新统计
        self.stats['total'] += before
        self.stats['kept'] += kept
        logger.info(f"清洗块: {before}→{kept} 行保留")
        return df

    def run(self):
        """
        主流程：
          - 检查源文件
          - 分块读取并清洗
          - 输出到目标文件
          - 打印总体统计
        """
        if not os.path.exists(self.src):
            logger.error(f"源文件不存在: {self.src}")
            return
        # 删除旧文件
        if os.path.exists(self.dst):
            os.remove(self.dst)

        header = True
        start = time.time()
        # 分块读取并清洗
        for chunk in pd.read_csv(self.src, chunksize=self.chunk_size):
            clean_chunk = self._clean(chunk)
            if not clean_chunk.empty:
                clean_chunk.to_csv(
                    self.dst,
                    mode='w' if header else 'a',
                    index=False,
                    header=header
                )
                header = False
        elapsed = time.time() - start
        kept, total = self.stats['kept'], self.stats['total']
        logger.info(
            f"完成: 共{total}行, 保留{kept}行({kept/total:.2%})，用时{elapsed:.1f}s"
        )

if __name__ == '__main__':
    # 从用户输入获取文件路径，提供默认值
    src = input("输入源CSV路径 (默认: game_events.csv): ") or "game_events.csv"
    dst = input("输入输出CSV路径 (默认: cleaned_game_events.csv): ") or "cleaned_game_events.csv"
    DataCleaner(src, dst).run()
