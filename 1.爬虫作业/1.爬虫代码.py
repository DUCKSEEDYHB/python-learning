# -*- coding: utf-8 -*-
import requests
import time
import json
import os
import re
import random
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ===================== 核心配置（精准匹配需求） =====================
SAVE_DIR = r"E:\py\practice\zpy"          
PDF_SAVE_DIR = os.path.join(SAVE_DIR, "pdf")  
MAX_PAGE = 311                             # 网页实际总页数（精准爬取无无效页）
TARGET_DATA_COUNT = 7762                   # 目标爬取全部7762条
BATCH_SIZE = 20                            
RETRY_TIMES = 3                            
MIN_DELAY = 1.5                            
MAX_DELAY = 3                              
RESUME_FILE = os.path.join(SAVE_DIR, "resume.txt")  
EXCEL_FILE = os.path.join(SAVE_DIR, "上交所债券发行公告数据.xlsx")  

# ===================== 初始化 =====================
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(PDF_SAVE_DIR, exist_ok=True)

code = []
short_name = []
name = []
set_time = []
pdf_names = []

# 请求会话优化
session = requests.Session()
retry_strategy = Retry(
    total=RETRY_TIMES,
    backoff_factor=0.8,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
session.mount("https://", HTTPAdapter(max_retries=retry_strategy, pool_connections=10))

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/142.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Firefox/139.0'
]

# 读取断点
start_page = 1
if os.path.exists(RESUME_FILE):
    with open(RESUME_FILE, "r", encoding="utf-8") as f:
        resume_page = f.read().strip()
        if resume_page.isdigit() and int(resume_page) > 1:
            start_page = int(resume_page)
    print(f"【断点续爬】从第 {start_page} 页开始")
else:
    print(f"【首次爬取】从第 1 页开始，目标 {TARGET_DATA_COUNT} 条发行公告")

# ===================== 工具函数：下载PDF（核心修正） =====================
def download_pdf(pdf_url, save_name):
    """
    确保下载的PDF文件名 = 债券简称_公告标题.pdf
    :param save_name: 传入的是已经按“债券简称_公告标题”生成的名称
    """
    # 仅处理特殊字符，不修改核心命名规则（和Excel中PDF名称完全一致）
    safe_name = re.sub(r'[\\/:*?"<>|]', '_', save_name)
    save_path = os.path.join(PDF_SAVE_DIR, f"{safe_name}.pdf")

    # 已存在则跳过
    if os.path.exists(save_path):
        print(f"📄 已存在：{safe_name}.pdf")
        return "已存在"

    # 无链接则标记
    if not pdf_url:
        print(f"⚠️  无链接：{safe_name}")
        return "无链接"

    # 下载逻辑
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://www.sse.com.cn/',
            'Accept': 'application/pdf, */*'
        }
        response = session.get(pdf_url, headers=headers, timeout=20)
        response.raise_for_status()

        with open(save_path, 'wb') as f:
            f.write(response.content)
        print(f"✅ 下载成功：{safe_name}.pdf")
        return "成功"
    except Exception as e:
        print(f"❌ 下载失败：{safe_name} - {str(e)[:50]}")
        return f"失败：{str(e)[:20]}"

# ===================== 核心爬取函数（精准筛选） =====================
def crawl_page(page_num):
    """
    精准筛选：
    1. 接口层筛选分类为发行公告
    2. 本地筛选标题含连续"发行公告"四字
    """
    print(f"\n【请求中】第 {page_num} 页")
    try:
        params = {
            'jsonCallBack': f'jsonCallback{random.randint(10000000, 99999999)}',
            'isPagination': 'true',
            'pageHelp.pageSize': '25',
            'pageHelp.cacheSize': '1',
            'type': 'inParams',
            'sqlId': 'BS_ZQ_GGLL',
            'sseDate': '2020-01-01 00:00:00',
            'sseDateEnd': '2024-12-31 23:59:59',
            'securityCode': '',
            'title': '发行公告',  # 接口层筛选分类为发行公告
            'orgBulletinType': '1101',
            'bondType': 'COMPANY_BOND_BULLETIN',
            'order': 'sseDate|desc,securityCode|asc,bulletinId|asc',
            'pageHelp.pageNo': str(page_num),
            'pageHelp.beginPage': str(page_num),
            'pageHelp.endPage': str(page_num),
            '_': str(int(time.time() * 1000))
        }

        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Referer': 'https://www.sse.com.cn/',
            'Host': 'query.sse.com.cn'
        }

        url = 'https://query.sse.com.cn/commonSoaQuery.do'
        response = session.get(url, params=params, headers=headers, timeout=15)
        response.raise_for_status()

        # 解析JSON
        json_text = response.text[response.text.index('(')+1 : response.text.rindex(')')]
        data = json.loads(json_text)
        page_data = data["pageHelp"]["data"]

        # 本地精准筛选：标题包含连续的"发行公告"四字
        result = []
        for item in page_data:
            title = item.get("title", "").strip()
            # 核心规则：标题中存在连续的"发行公告"（不限制位置/结尾）
            if "发行公告" in title:
                pdf_relative_url = item.get("url", "")
                full_pdf_url = f"https://static.sse.com.cn{pdf_relative_url}" if pdf_relative_url else ""
                # 统一命名规则：债券简称_公告标题（和下载的PDF文件名完全一致）
                pdf_file_name = f"{item.get('securityAbbr', '')}_{title}"
                
                result.append({
                    "证券代码": item.get("securityCode", ""),
                    "证券简称": item.get("securityAbbr", ""),
                    "公告标题": title,
                    "发布日期": item.get("sseDate", "")[:10],
                    "PDF链接": full_pdf_url,
                    "PDF名称": pdf_file_name  # Excel中记录的名称
                })
        print(f"【解析完成】第 {page_num} 页：{len(result)} 条符合条件的发行公告")
        return result

    except Exception as e:
        print(f"【爬取失败】第 {page_num} 页：{str(e)}")
        return None

# ===================== 主逻辑（确保爬满7762条） =====================
if __name__ == "__main__":
    is_stop = False  # 终止标记
    try:
        for page in range(start_page, MAX_PAGE + 1):
            if is_stop:
                break
                
            # 爬取当前页
            page_result = crawl_page(page)
            if not page_result:
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
                continue

            # 处理当前页所有数据（避免漏爬）
            new_count = 0
            for item in page_result:
                # 先追加数据，再判断是否终止
                code.append(item["证券代码"])
                short_name.append(item["证券简称"])
                name.append(item["公告标题"])
                set_time.append(item["发布日期"])
                pdf_names.append(item["PDF名称"])  # 记录统一的命名

                # 下载PDF：直接传入统一命名的名称，确保文件名一致
                download_status = download_pdf(item["PDF链接"], item["PDF名称"])
                new_count += 1

                # 达到目标量，标记终止
                if len(code) >= TARGET_DATA_COUNT:
                    is_stop = True
                    break

            # 打印进度
            print(f"✅ 累计：{len(code)} 条（新增 {new_count} 条）")

            # 分批保存
            if (page % BATCH_SIZE == 0) or (page == MAX_PAGE) or is_stop:
                df = pd.DataFrame({
                    "证券代码": code,
                    "证券简称": short_name,
                    "公告标题": name,
                    "发布日期": set_time,
                    "PDF名称": pdf_names  # Excel中显示的名称和下载的PDF文件名一致
                })
                df.to_excel(EXCEL_FILE, index=False, engine='openpyxl')
                print(f"💾 已保存到：{EXCEL_FILE}")

                # 记录断点
                with open(RESUME_FILE, "w", encoding="utf-8") as f:
                    f.write(str(page + 1))
                print(f"📌 断点：下次从第 {page + 1} 页开始")

            # 随机延迟
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        # 最终保存
        df_final = pd.DataFrame({
            "证券代码": code,
            "证券简称": short_name,
            "公告标题": name,
            "发布日期": set_time,
            "PDF名称": pdf_names
        })
        df_final.to_excel(EXCEL_FILE, index=False, engine='openpyxl')

        # 爬取完成，清理断点文件
        if os.path.exists(RESUME_FILE):
            os.remove(RESUME_FILE)

        print(f"\n📊 最终结果：共爬取 {len(code)} 条符合条件的发行公告")
        print(f"📁 Excel文件：{EXCEL_FILE}")
        print(f"📂 PDF目录：{PDF_SAVE_DIR}")

    except KeyboardInterrupt:
        # 手动中断时保存
        df = pd.DataFrame({
            "证券代码": code,
            "证券简称": short_name,
            "公告标题": name,
            "发布日期": set_time,
            "PDF名称": pdf_names
        })
        df.to_excel(EXCEL_FILE, index=False, engine='openpyxl')
        print(f"\n⚠️  手动中断，已保存 {len(code)} 条")

    finally:
        session.close()
        print("🔌 会话关闭，程序结束")