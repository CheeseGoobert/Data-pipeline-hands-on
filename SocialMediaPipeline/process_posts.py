"""
Social Media Posts Processing Pipeline
- Reads raw data, performs sentiment analysis, and stores results in SQLite
"""

import pandas as pd
import sqlite3
import os

# ============================================================
# 1. 读取数据
# ============================================================
def load_data():
    """从 raw_data 目录读取 posts.txt 和 users.txt"""
    posts_df = pd.read_csv('./raw_data/posts.txt')
    users_df = pd.read_csv('./raw_data/users.txt')
    print(f"Loaded {len(posts_df)} posts and {len(users_df)} users")
    return posts_df, users_df

# ============================================================
# 2. 数据清洗
# ============================================================
def clean_data(posts_df):
    """删除 content 列为空的行"""
    original_count = len(posts_df)
    cleaned_df = posts_df[posts_df['content'].notna() & (posts_df['content'].str.strip() != '')]
    print(f"Cleaned: removed {original_count - len(cleaned_df)} empty posts")
    return cleaned_df

# ============================================================
# 3. 情感分析
# ============================================================
def analyze_sentiment(content):
    """根据关键词判断情感倾向"""
    content_lower = content.lower()
    
    positive_keywords = ['love', 'great', 'awesome', 'good', 'excellent', 'amazing']
    negative_keywords = ['hate', 'terrible', 'bad', 'awful', 'worst', 'horrible']
    
    if any(word in content_lower for word in positive_keywords):
        return 'positive'
    elif any(word in content_lower for word in negative_keywords):
        return 'negative'
    else:
        return 'neutral'

def add_sentiment(posts_df):
    """为每条帖子添加情感标签"""
    posts_df['sentiment'] = posts_df['content'].apply(analyze_sentiment)
    print(f"Sentiment analysis complete: {posts_df['sentiment'].value_counts().to_dict()}")
    return posts_df

# ============================================================
# 4. 数据合并
# ============================================================
def merge_data(posts_df, users_df):
    """根据 user_id 合并帖子和用户数据"""
    merged_df = posts_df.merge(users_df, on='user_id', how='left')
    print(f"Merged data: {len(merged_df)} rows")
    return merged_df

# ============================================================
# 5. 写入数据库
# ============================================================
def save_to_database(merged_df):
    """将合并后的数据写入 SQLite 数据库"""
    db_path = 'social_media.db'
    
    # 删除旧数据库文件（如果存在）
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    
    # 写入主表
    merged_df.to_sql('analyzed_posts', conn, index=False, if_exists='replace')
    print(f"Saved {len(merged_df)} rows to 'analyzed_posts' table")
    
    conn.close()
    return db_path

# ============================================================
# 6. 创建汇总表
# ============================================================
def create_summary_table(db_path):
    """创建情感汇总表"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 创建汇总表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sentiment_summary AS
        SELECT sentiment, COUNT(*) as post_count
        FROM analyzed_posts
        GROUP BY sentiment
    ''')
    
    conn.commit()
    
    # 验证汇总结果
    summary_df = pd.read_sql('SELECT * FROM sentiment_summary', conn)
    print(f"Sentiment summary created: {summary_df.to_dict('records')}")
    
    conn.close()

# ============================================================
# 主流程
# ============================================================
def main():
    print("=" * 50)
    print("Starting Social Media Pipeline")
    print("=" * 50)
    
    # 1. 读取数据
    posts_df, users_df = load_data()
    
    # 2. 数据清洗
    posts_df = clean_data(posts_df)
    
    # 3. 情感分析
    posts_df = add_sentiment(posts_df)
    
    # 4. 数据合并
    merged_df = merge_data(posts_df, users_df)
    
    # 5. 写入数据库
    db_path = save_to_database(merged_df)
    
    # 6. 创建汇总表
    create_summary_table(db_path)
    
    print("=" * 50)
    print("Pipeline completed successfully!")
    print("=" * 50)

if __name__ == '__main__':
    main()
