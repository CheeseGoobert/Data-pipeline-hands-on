"""
Flask Web Application for Social Media Analytics Dashboard
"""

from flask import Flask, render_template_string
import sqlite3

app = Flask(__name__)
DB_PATH = 'social_media.db'


def get_db_connection():
    """创建数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ============================================================
# 主页 - 展示所有帖子数据
# ============================================================
@app.route('/')
def index():
    conn = get_db_connection()
    # 查询合并后的数据，展示关键列
    posts = conn.execute('''
        SELECT post_id, content, username, follower_count, sentiment, timestamp
        FROM analyzed_posts
        ORDER BY timestamp DESC
    ''').fetchall()
    conn.close()
    
    # HTML 模板
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>Social Media Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
            h1 { color: #333; }
            table { border-collapse: collapse; width: 100%; background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
            th { background-color: #4CAF50; color: white; }
            tr:nth-child(even) { background-color: #f9f9f9; }
            .positive { color: green; font-weight: bold; }
            .negative { color: red; font-weight: bold; }
            .neutral { color: gray; }
            nav { margin-bottom: 20px; }
            nav a { margin-right: 20px; text-decoration: none; color: #4CAF50; font-size: 18px; }
        </style>
    </head>
    <body>
        <nav>
            <a href="/">All Posts</a>
            <a href="/summary">Summary</a>
        </nav>
        <h1>Social Media Posts</h1>
        <p>Total posts: {{ posts|length }}</p>
        <table>
            <tr>
                <th>Post ID</th>
                <th>Content</th>
                <th>Username</th>
                <th>Followers</th>
                <th>Sentiment</th>
                <th>Timestamp</th>
            </tr>
            {% for post in posts %}
            <tr>
                <td>{{ post.post_id }}</td>
                <td>{{ post.content }}</td>
                <td>{{ post.username }}</td>
                <td>{{ post.follower_count }}</td>
                <td class="{{ post.sentiment }}">{{ post.sentiment.upper() }}</td>
                <td>{{ post.timestamp }}</td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    '''
    return render_template_string(html, posts=posts)


# ============================================================
# 摘要页 - 展示情感统计
# ============================================================
@app.route('/summary')
def summary():
    conn = get_db_connection()
    summary_data = conn.execute('SELECT * FROM sentiment_summary').fetchall()
    total = conn.execute('SELECT COUNT(*) as total FROM analyzed_posts').fetchone()
    conn.close()
    
    html = '''
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <title>Sentiment Summary</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }
            h1 { color: #333; }
            nav { margin-bottom: 20px; }
            nav a { margin-right: 20px; text-decoration: none; color: #4CAF50; font-size: 18px; }
            .card { background: white; padding: 20px; margin: 10px 0; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            .positive { border-left: 5px solid green; }
            .negative { border-left: 5px solid red; }
            .neutral { border-left: 5px solid gray; }
            .count { font-size: 36px; font-weight: bold; }
            .label { color: #666; font-size: 14px; }
        </style>
    </head>
    <body>
        <nav>
            <a href="/">All Posts</a>
            <a href="/summary">Summary</a>
        </nav>
        <h1>Sentiment Summary</h1>
        <p>Total posts analyzed: <strong>{{ total.total }}</strong></p>
        
        {% for item in summary_data %}
        <div class="card {{ item.sentiment }}">
            <div class="count">{{ item.post_count }}</div>
            <div class="label">{{ item.sentiment|upper() }}</div>
        </div>
        {% endfor %}
    </body>
    </html>
    '''
    return render_template_string(html, summary_data=summary_data, total=total)


if __name__ == '__main__':
    print("=" * 50)
    print("Starting Social Media Dashboard")
    print("Open http://127.0.0.1:5000 in your browser")
    print("=" * 50)
    app.run(debug=True, port=5000)
