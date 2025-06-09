import streamlit as st
import pandas as pd
import sqlite3  # SQLite connection
import plotly.express as px
import numpy as np
from datetime import datetime
import warnings
import re

# 忽略警告信息
warnings.filterwarnings('ignore')

# 页面基本配置
st.set_page_config(
    page_title="游戏数据分析仪表板",
    layout="wide",
    initial_sidebar_state="expanded"
)

class GameAnalyticsDashboard:
    """
    游戏数据分析仪表板
    从 SQLite 加载数据，计算关键指标，并使用 Plotly 可视化
    """
    def __init__(self, db_path):
        self.db_path = db_path
        self.data = None

    def load_data(self):
        """
        从 SQLite 数据库中加载事件表
        自动支持 events 或 cleaned_events 表
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                tables = pd.read_sql_query(
                    "SELECT name FROM sqlite_master WHERE type='table'", conn
                )
                # 判断表名
                if 'events' in tables['name'].values:
                    table = 'events'
                elif 'cleaned_events' in tables['name'].values:
                    table = 'cleaned_events'
                else:
                    st.error("数据库中找不到事件表")
                    return False

                # 获取列信息
                cols_info = pd.read_sql_query(f"PRAGMA table_info({table})", conn)
                cols = cols_info['name'].tolist()

                # 构建 SELECT 字段并重命名
                select = []
                # 必需字段映射
                required = {
                    'event_id': ['event_id', 'EventID'],
                    'player_id': ['player_id', 'PlayerID'],
                    'event_timestamp': ['event_timestamp', 'EventTimestamp'],
                    'event_type': ['event_type', 'EventType']
                }
                for alias, opts in required.items():
                    for c in opts:
                        if c in cols:
                            select.append(f"{c} AS {alias}")
                            break

                # 可选字段处理
                optional = {
                    'event_details': ['event_details', 'EventDetails'],
                    'device_type': ['device_type', 'DeviceType'],
                    'location': ['location', 'Location'],
                    'purchase_amount': ['purchase_amount'],
                    'event_date': [],
                    'event_hour': []
                }
                for alias, opts in optional.items():
                    found = False
                    for c in opts:
                        if c in cols:
                            select.append(f"{c} AS {alias}")
                            found = True
                            break
                    if not found:
                        # 默认生成
                        if alias == 'event_details':
                            select.append("'' AS event_details")
                        elif alias == 'device_type':
                            select.append("'Unknown' AS device_type")
                        elif alias == 'location':
                            select.append("'Unknown' AS location")
                        elif alias == 'purchase_amount':
                            select.append("0.0 AS purchase_amount")
                        elif alias == 'event_date':
                            select.append("DATE(event_timestamp) AS event_date")
                        elif alias == 'event_hour':
                            select.append("CAST(strftime('%H', event_timestamp) AS INTEGER) AS event_hour")

                query = f"SELECT {', '.join(select)} FROM {table} WHERE event_timestamp IS NOT NULL"
                self.data = pd.read_sql_query(query, conn)
                # 转换时间类型
                self.data['event_timestamp'] = pd.to_datetime(self.data['event_timestamp'], errors='coerce')
                self.data['event_date'] = pd.to_datetime(self.data['event_date'], errors='coerce')

                return True
        except Exception as e:
            st.error(f"加载数据失败: {e}")
            return False

    def calculate_session_duration(self):
        """
        计算每个玩家每天的会话时长（分钟）
        """
        # 按玩家和日期计算最早和最晚事件
        df = self.data.copy()
        df['date_only'] = df['event_date'].dt.date
        stats = df.groupby(['player_id', 'date_only'])['event_timestamp']\
                  .agg(['min', 'max', 'count']).reset_index()
        stats = stats[stats['count'] > 1]
        stats['duration_minutes'] = (stats['max'] - stats['min']).dt.total_seconds() / 60
        stats['duration_minutes'] = stats['duration_minutes'].clip(lower=1)
        return stats[['player_id', 'date_only', 'duration_minutes']]

    def calculate_metrics(self):
        """
        计算并返回核心指标：
          - 每日活跃用户数
          - 平均会话时长
          - 每日收入
          - 社交互动/会话
        """
        df = self.data
        # 1. DAU
        dau = df.groupby('event_date')['player_id'].nunique().reset_index()
        dau.columns = ['date', 'dau']
        # 2. 会话时长
        sessions = self.calculate_session_duration()
        avg_dur = sessions.groupby('date_only')['duration_minutes'].mean().reset_index()
        avg_dur.columns = ['date', 'avg_duration']
        # 3. 收入
        mask = df['event_type'] == 'InAppPurchase'
        df.loc[mask, 'purchase_amount'] = df.loc[mask, 'event_details']\
            .apply(lambda x: float(re.search(r"Amount:\s*\$(\d+\.?\d*)", x).group(1)) if pd.notna(x) and re.search(r"Amount:\s*\$(\d+\.?\d*)", x) else 0)
        revenue = df[mask].groupby('event_date')['purchase_amount'].sum().reset_index()
        revenue.columns = ['date', 'revenue']
        # 4. 社交互动/会话
        social = df[df['event_type'] == 'SocialInteraction'].groupby('event_date').size().reset_index(name='social_count')
        total_sessions = sessions.groupby('date_only').size().reset_index(name='sessions')
        social_per = pd.merge(social, total_sessions, left_on='event_date', right_on='date_only', how='inner')
        social_per['social_per_session'] = social_per['social_count'] / social_per['sessions']
        social_per = social_per[['event_date', 'social_per_session']].rename(columns={'event_date':'date'})
        return {'dau': dau, 'duration': avg_dur, 'revenue': revenue, 'social': social_per}

    # 绘图函数略，保留原实现即可

# 主流程
st.title("游戏数据分析仪表板")
if 'dashboard' not in st.session_state:
    st.session_state['dashboard'] = None

db_file = st.sidebar.text_input("数据库路径", value="game_data.db")
if st.sidebar.button("加载数据"):
    dash = GameAnalyticsDashboard(db_file)
    if dash.load_data():
        st.session_state['dashboard'] = dash
        st.sidebar.success("数据加载成功")

if st.session_state['dashboard']:
    metrics = st.session_state['dashboard'].calculate_metrics()
    st.subheader("每日活跃用户数")
    st.line_chart(metrics['dau'].set_index('date'))
    st.subheader("平均会话时长 (分钟)")
    st.line_chart(metrics['duration'].set_index('date'))
    st.subheader("每日收入 (元)")
    st.bar_chart(metrics['revenue'].set_index('date'))
    st.subheader("社交互动/会话")
    st.line_chart(metrics['social'].set_index('date'))

else:
    st.info("请在侧边栏加载数据后查看分析")
