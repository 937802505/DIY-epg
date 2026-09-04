#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电视猫 EPG 爬虫 - 全频道今日节目单
================================================
从 https://www.tvmao.com 抓取所有频道今日 EPG，输出 XMLTV 格式（.xml + .xml.gz）。

输出格式完全参考 辽宁epg格式：
  - 每个频道根据 epg_data.json 的 name 字段生成多个 display-name
  - 时间格式：YYYYMMDDHHMMSS +0800
  - 仅抓取今日节目单

用法：
  python tvmao_epg.py                    # 全部频道
  python tvmao_epg.py --output EPG       # 指定输出目录
  python tvmao_epg.py --channels CCTV    # 仅抓取指定分组
"""

import argparse
import gzip
import io
import json
import os
import re
import sys
import time
import threading

if sys.stdout and sys.stdout.encoding and sys.stdout.encoding.lower() in ('gbk', 'cp936', 'gb2312'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# ─────────────────────────────── 配置 ───────────────────────────────
BASE_URL = "https://www.tvmao.com"

# 所有频道分组及其 URL 前缀
# 央视组: /program/CCTV-{CHANNEL}-w{weekday}.html
# 卫视组: /program_satellite/{CHANNEL}-w{weekday}.html
# 中数传媒: /program/CCTVPAYFEE-{CHANNEL}-w{weekday}.html
# 教育台: /program/CETV-{CHANNEL}-w{weekday}.html
# 华诚付费: /program/CHC-{CHANNEL}-w{weekday}.html
# 北广传媒: /program/BAMC-{CHANNEL}-w{weekday}.html
# 华数传媒: /program/HSCM-{CHANNEL}-w{weekday}.html
# 上海: /program/SHHAI-{CHANNEL}-w{weekday}.html
# 北京: /program/BTV-{CHANNEL}-w{weekday}.html

# 分组发现页面（这些页面侧栏列出所有频道链接）
GROUP_DISCOVERY = {
    'CCTV':      '/program/CCTV',
    'CCTVPAYFEE': '/program/CCTVPAYFEE',
    'CETV':      '/program/CETV',
    'CHC':       '/program/CHC',
    'BAMC':      '/program/BAMC',
    'HSCM':      '/program/HSCM',
    'BTV':       '/program/BTV',
}

# 卫视频道（通过侧栏直接发现，URL前缀为 /program_satellite/）
SATELLITE_URL_PREFIX = '/program_satellite'

# 省份频道分组页面（这些页面侧栏列出该省所有频道）
PROVINCE_GROUPS = {
    'LNTV':  '/program/LNTV',   # 辽宁
    'SDTV':  '/program/SDTV',   # 山东
    'HNTV':  '/program/HNTV',   # 河南
    'HUBEI': '/program/HUBEI',  # 湖北
    'HLJTV': '/program/HLJTV',  # 黑龙江
    'JILIN': '/program/JILIN',  # 吉林
    'SCTV':  '/program/SCTV',   # 四川
    'TJTV':  '/program/TJTV',   # 天津
    'HEBEI': '/program/HEBEI',  # 河北
    'AHTV':  '/program/AHTV',   # 安徽
    'JSTV':  '/program/JSTV',   # 江苏
    'ZJTV':  '/program/ZJTV',   # 浙江
    'GDTV':  '/program/GDTV',   # 广东
    'HUNANTV': '/program/HUNANTV',  # 湖南
    'FJTV':  '/program/FJTV',   # 福建
    'JXTV':  '/program/JXTV',   # 江西
    'NMGTV': '/program/NMGTV',  # 内蒙古
    'XJTV':  '/program/XJTV',   # 新疆
    'XIZANGTV': '/program/XIZANGTV',  # 西藏
    'QHTV':  '/program/QHTV',   # 青海
    'NXTV':  '/program/NXTV',   # 宁夏
    'GUIZOUTV': '/program/GUIZOUTV',  # 贵州
    'YNTV':  '/program/YNTV',   # 云南
    'SXTV':  '/program/SXTV',   # 山西
    'SHXITV': '/program/SHXITV',  # 陕西
    'CCQTV': '/program/CCQTV',  # 重庆
    'GSTV':  '/program/GSTV',   # 甘肃
    'TCTC':  '/program/TCTC',   # 海南
    'GUANXI': '/program/GUANXI',  # 广西
    'DFMV':  '/program/DFMV',   # 上海
}

# ─────────────────────────── 共享工具 ───────────────────────────
ILLEGAL_XML_CHARS_RE = re.compile(
    u'[\x00-\x08\x0b\x0c\x0e-\x1F\x7F-\x84\x86-\x9f'
    u'\ud800-\udfff\ufdd0-\ufdef\ufffe\uffff]'
)


def clean_xml_text(text):
    if not text:
        return ""
    return ILLEGAL_XML_CHARS_RE.sub('', text).strip()


def indent_xml(elem, level=0):
    i = "\n" + level * "  "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
        for child in elem:
            indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = i
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = i


def write_xml_and_gz(root, xml_path, gz_path):
    indent_xml(root)
    tree = ET.ElementTree(root)
    buf = io.BytesIO()
    tree.write(buf, encoding='utf-8', xml_declaration=False, method='xml')
    xml_body = buf.getvalue().decode('utf-8')
    content = '<?xml version="1.0" encoding="utf-8"?>\n' + xml_body + '\n'
    with open(xml_path, 'w', encoding='utf-8') as f:
        f.write(content)
    with gzip.open(gz_path, 'wt', encoding='utf-8') as f:
        f.write(content)
    print(f"  已保存: {xml_path} ({os.path.getsize(xml_path):,} bytes)")
    print(f"  已保存: {gz_path} ({os.path.getsize(gz_path):,} bytes)")


# ─────────────────────────── 频道名称映射 ───────────────────────────
class ChannelNameMapper:
    """从 epg_data.json 加载频道名称映射，支持多别名匹配"""

    def __init__(self, epg_data_path):
        self.epg_map = {}       # epgid -> {logo, names: [alias1, alias2, ...]}
        self.name_to_epgid = {} # alias -> epgid
        if epg_data_path and os.path.exists(epg_data_path):
            self._load(epg_data_path)

    def _load(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for entry in data.get('epgs', []):
            epgid = entry.get('epgid', '')
            name_str = entry.get('name', '')
            logo = entry.get('logo', '')
            if not epgid or not name_str:
                continue
            names = [n.strip() for n in name_str.split(',') if n.strip()]
            self.epg_map[epgid] = {'logo': logo, 'names': names}
            for name in names:
                self.name_to_epgid[name] = epgid

    def match_channel(self, tvmao_name):
        """用 tvmao 的频道名匹配 epgid，返回 (epgid, matched_name) 或 None"""
        if not tvmao_name:
            return None
        clean = tvmao_name.strip()
        # 直接匹配
        if clean in self.name_to_epgid:
            return self.name_to_epgid[clean], clean
        # 去除空格/括号后匹配
        simplified = re.sub(r'[\s（）()\[\]【】]+', '', clean)
        for name, epgid in self.name_to_epgid.items():
            name_simplified = re.sub(r'[\s（）()\[\]【】]+', '', name)
            if name_simplified == simplified:
                return epgid, name
        # 模糊匹配：tvmao名包含在某个别名中
        for name, epgid in self.name_to_epgid.items():
            if clean in name or name in clean:
                return epgid, name
        return None

    def get_aliases(self, epgid):
        """获取某个 epgid 的所有别名"""
        entry = self.epg_map.get(epgid)
        if entry:
            return entry['names']
        return [epgid]

    def get_logo(self, epgid):
        entry = self.epg_map.get(epgid)
        if entry:
            return entry.get('logo', '')
        return ''


# ─────────────────────────── HTTP 会话 ───────────────────────────
def create_session():
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.tvmao.com/',
    })
    return session


def fetch_page(session, url, retries=3):
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=20, allow_redirects=True)
            resp.raise_for_status()
            return resp.text
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.5 * (attempt + 1))
    return None


# ─────────────────────────── 频道发现 ───────────────────────────
def discover_today_weekday(session):
    """从任意频道页面检测今天对应的 weekday 数字 (1-7, 1=周一)"""
    url = f"{BASE_URL}/program/CCTV"
    html = fetch_page(session, url)
    if not html:
        return datetime.now().isoweekday(), datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    soup = BeautifulSoup(html, 'lxml')
    # 找 weekcur 高亮的日期
    week_links = soup.select('dl.week-menu dd')
    today_w = None
    today_date = None
    for dd in week_links:
        cls = dd.get('class', [])
        link = dd.find('a')
        date_span = dd.find('span')
        if 'weekcur' in cls and link:
            href = link.get('href', '')
            m = re.search(r'-w(\d+)\.html', href)
            if m:
                today_w = int(m.group(1))
            if date_span:
                date_text = date_span.get_text(strip=True)
                try:
                    today_date = datetime.strptime(f"2026-{date_text}", '%Y-%m-%d')
                except ValueError:
                    pass
            break
    if today_w is None:
        today_w = datetime.now().isoweekday()
    if today_date is None:
        today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    return today_w, today_date


def discover_channels_from_page(session, discovery_url):
    """从一个分组页面的侧栏发现所有频道链接和名称"""
    html = fetch_page(session, BASE_URL + discovery_url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    channels = []
    seen = set()
    # 查找侧栏中的频道链接
    for link in soup.select('a.black_link'):
        href = link.get('href', '')
        name = link.get_text(strip=True)
        if not href or not name:
            continue
        # 只要包含 -w{数字}.html 的频道链接
        if not re.search(r'-w\d+\.html', href):
            continue
        # 提取频道URL前缀（去掉-w数字.html部分）
        prefix = re.sub(r'-w\d+\.html$', '', href)
        if prefix in seen:
            continue
        seen.add(prefix)
        channels.append({
            'url_prefix': prefix,
            'name': name,
        })
    return channels


def discover_all_channels(session):
    """发现所有频道"""
    all_channels = {}  # url_prefix -> name
    print("\n=== 发现频道列表 ===")

    # 1. 央视组
    print("  正在发现 CCTV 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['CCTV']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  CCTV: {len([c for c in all_channels if '/CCTV-' in c])} 频道")

    # 2. 卫视
    print("  正在发现 卫视 频道...")
    for ch in discover_channels_from_page(session, '/program_satellite/AHTV1-w5.html'):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  卫视: {len([c for c in all_channels if '/program_satellite/' in c])} 频道")

    # 3. 中数传媒
    print("  正在发现 CCTVPAYFEE 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['CCTVPAYFEE']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  CCTVPAYFEE: {len([c for c in all_channels if '/CCTVPAYFEE-' in c])} 频道")

    # 4. 教育台
    print("  正在发现 CETV 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['CETV']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  CETV: {len([c for c in all_channels if '/CETV-' in c])} 频道")

    # 5. 华诚付费
    print("  正在发现 CHC 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['CHC']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  CHC: {len([c for c in all_channels if '/CHC-' in c])} 频道")

    # 6. 北广传媒
    print("  正在发现 BAMC 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['BAMC']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  BAMC: {len([c for c in all_channels if '/BAMC-' in c])} 频道")

    # 7. 华数传媒
    print("  正在发现 HSCM 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['HSCM']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  HSCM: {len([c for c in all_channels if '/HSCM-' in c])} 频道")

    # 8. 北京
    print("  正在发现 BTV 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['BTV']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  BTV: {len([c for c in all_channels if '/BTV-' in c])} 频道")

    # 9. 省份频道
    for prov_name, prov_url in PROVINCE_GROUPS.items():
        print(f"  正在发现 {prov_name} 频道...")
        channels = discover_channels_from_page(session, prov_url)
        before = len(all_channels)
        for ch in channels:
            if ch['url_prefix'] not in all_channels:
                all_channels[ch['url_prefix']] = ch['name']
        print(f"  {prov_name}: +{len(all_channels) - before} 频道")

    print(f"\n✓ 共发现 {len(all_channels)} 个频道")
    return all_channels


# ─────────────────────────── EPG 解析 ───────────────────────────
def parse_channel_epg(html, today_w, today_date):
    """
    解析一个频道今日的 EPG 页面，返回程序列表。
    HTML 结构:
      <ul id="pgrow">
        <li><div class="over_hide">
          <span class="am/pm">HH:MM</span>
          <span class="p_show">节目名</span>
        </div></li>
      </ul>
    """
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    programs = []

    # 找到 weekcur 高亮的那一天（即今日）
    week_links = soup.select('dl.week-menu dd')
    target_date = today_date

    # 查找所有 <li> 在 <ul id="pgrow"> 内
    pgrow = soup.find('ul', id='pgrow')
    if not pgrow:
        return []

    current_hour = 0
    for li in pgrow.find_all('li', recursive=False):
        # 跳过分隔标签
        if li.get('id') in ('noon', 'night'):
            continue
        over_hide = li.find('div', class_='over_hide')
        if not over_hide:
            continue

        # 获取时间
        time_span = over_hide.find('span', class_=re.compile(r'^(am|pm)$'))
        if not time_span:
            continue
        time_text = time_span.get_text(strip=True)
        m = re.match(r'(\d{1,2}):(\d{2})', time_text)
        if not m:
            continue
        hour, minute = int(m.group(1)), int(m.group(2))

        # 获取节目名
        p_show = over_hide.find('span', class_='p_show')
        if not p_show:
            continue
        title = p_show.get_text(strip=True)
        if not title:
            continue

        # 判断是否跨天（小时小于前一个节目小时 -> 次日凌晨段）
        if hour < current_hour:
            target_date = target_date + timedelta(days=1)
        current_hour = hour

        start = target_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        programs.append({'title': title, 'start': start})

    # 计算每个节目的结束时间（下一个节目的开始时间，最后一个节目到当日结束）
    for i in range(len(programs)):
        if i + 1 < len(programs):
            programs[i]['stop'] = programs[i + 1]['start']
        else:
            # 最后一个节目：如果已经到第二天，用第二天06:00；否则用当天23:59:59
            if programs[i]['start'].day != today_date.day:
                programs[i]['stop'] = programs[i]['start'].replace(hour=6, minute=0, second=0)
            else:
                programs[i]['stop'] = programs[i]['start'].replace(hour=23, minute=59, second=59)

    return programs


def fetch_channel_epg(session, url_prefix, today_w):
    """获取一个频道今日的 EPG"""
    url = f"{BASE_URL}{url_prefix}-w{today_w}.html"
    html = fetch_page(session, url)
    return html


# ─────────────────────────── 主流程 ───────────────────────────
class TvmaoEPGCrawler:
    def __init__(self, epg_data_path='epg_data.json', output_dir='EPG'):
        self.output_dir = output_dir
        self.epg_data_path = epg_data_path
        self.session = create_session()
        self.mapper = ChannelNameMapper(epg_data_path)
        self.today_w = None
        self.today_date = None
        self.lock = Lock()
        self.all_programs = []   # [{channel_id, title, start, stop}]
        self.channel_info = {}  # url_prefix -> {epgid, tvmao_name, aliases}

    def run(self, filter_groups=None):
        os.makedirs(self.output_dir, exist_ok=True)
        print("=" * 60)
        print("电视猫 EPG 爬虫启动")
        print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # 1. 检测今天星期
        print("\n=== 检测今天星期 ===")
        self.today_w, self.today_date = discover_today_weekday(self.session)
        print(f"  今天: w{self.today_w} ({self.today_date.strftime('%Y-%m-%d')})")

        # 2. 发现所有频道
        all_channels = discover_all_channels(self.session)

        # 3. 过滤频道
        if filter_groups:
            filtered = {}
            for prefix, name in all_channels.items():
                for grp in filter_groups:
                    if f'/{grp}-' in prefix or (grp == 'satellite' and '/program_satellite/' in prefix):
                        filtered[prefix] = name
                        break
            all_channels = filtered
            print(f"\n  过滤后: {len(all_channels)} 频道")

        # 4. 匹配频道名称到 epgid
        print("\n=== 匹配频道名称 ===")
        matched_count = 0
        for prefix, tvmao_name in all_channels.items():
            result = self.mapper.match_channel(tvmao_name)
            if result:
                epgid, matched_name = result
                aliases = self.mapper.get_aliases(epgid)
                self.channel_info[prefix] = {
                    'epgid': epgid,
                    'tvmao_name': tvmao_name,
                    'aliases': aliases,
                    'matched_name': matched_name,
                }
                matched_count += 1
            else:
                # 未匹配到 epg_data，用 tvmao 原名
                self.channel_info[prefix] = {
                    'epgid': tvmao_name,
                    'tvmao_name': tvmao_name,
                    'aliases': [tvmao_name],
                    'matched_name': tvmao_name,
                }
        print(f"  匹配: {matched_count}/{len(all_channels)} 频道")

        # 5. 并发抓取所有频道 EPG
        print(f"\n=== 抓取今日 EPG ({len(all_channels)} 频道) ===")
        self._crawl_all_channels(all_channels)

        # 6. 去重
        print("\n=== 去重 ===")
        self._deduplicate()

        # 7. 生成 XMLTV
        print("\n=== 生成 XMLTV ===")
        self._generate_xmltv()

        print("\n" + "=" * 60)
        print("全部完成！")
        print("=" * 60)

    def _crawl_all_channels(self, all_channels):
        """并发抓取所有频道"""
        progress_lock = Lock()
        done_count = [0]
        total = len(all_channels)

        def fetch_one(prefix, name):
            html = fetch_channel_epg(self.session, prefix, self.today_w)
            if html:
                programs = parse_channel_epg(html, self.today_w, self.today_date)
                info = self.channel_info[prefix]
                with self.lock:
                    for p in programs:
                        self.all_programs.append({
                            'channel_id': info['epgid'],
                            'title': p['title'],
                            'start': p['start'],
                            'stop': p['stop'],
                        })
            with progress_lock:
                done_count[0] += 1
                if done_count[0] % 20 == 0 or done_count[0] == total:
                    print(f"  进度: {done_count[0]}/{total} ({done_count[0]*100//total}%)")

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(fetch_one, prefix, name) for prefix, name in all_channels.items()]
            for f in as_completed(futures):
                try:
                    f.result(timeout=30)
                except Exception as e:
                    pass

        print(f"✓ 抓取完成: {len(self.all_programs)} 条节目")

    def _deduplicate(self):
        seen = set()
        unique = []
        for p in self.all_programs:
            key = (p['channel_id'], p['start'].strftime('%Y%m%d%H%M'))
            if key not in seen:
                seen.add(key)
                unique.append(p)
        self.all_programs = unique
        print(f"  去重后: {len(self.all_programs)} 条节目")

    def _generate_xmltv(self):
        """生成辽宁epg格式的 XMLTV"""
        root = ET.Element('tv')
        root.set('generator-info-name', 'TVMAO_EPG')
        root.set('generator-info-url', 'https://www.tvmao.com')

        # 收集所有 epgid
        epgids = set()
        for p in self.all_programs:
            epgids.add(p['channel_id'])

        # 为每个 epgid 生成 channel 定义（每个别名一个 <channel> 元素，相同 id）
        for epgid in sorted(epgids):
            aliases = self.mapper.get_aliases(epgid)
            if not aliases:
                aliases = [epgid]
            for alias in aliases:
                ch = ET.SubElement(root, 'channel')
                ch.set('id', epgid)
                dn = ET.SubElement(ch, 'display-name')
                dn.set('lang', 'zh')
                dn.text = clean_xml_text(alias)

        def fmt(dt):
            if isinstance(dt, datetime):
                return dt.strftime('%Y%m%d%H%M%S') + ' +0800'
            return str(dt)

        # 生成节目
        for p in sorted(self.all_programs, key=lambda x: (x['channel_id'], fmt(x['start']))):
            title_text = clean_xml_text(p['title'])
            if not title_text:
                continue
            prog = ET.SubElement(root, 'programme')
            prog.set('start', fmt(p['start']))
            prog.set('stop', fmt(p['stop']))
            prog.set('channel', p['channel_id'])
            title_el = ET.SubElement(prog, 'title')
            title_el.set('lang', 'zh')
            title_el.text = title_text

        # 输出
        xml_path = os.path.join(self.output_dir, 'epg.xml')
        gz_path = os.path.join(self.output_dir, 'epg.xml.gz')
        write_xml_and_gz(root, xml_path, gz_path)

        # 统计
        channel_count = len(set(p['channel_id'] for p in self.all_programs))
        print(f"\n  频道数: {channel_count}")
        print(f"  节目数: {len(self.all_programs)}")


# ─────────────────────────── 入口 ───────────────────────────
def main():
    parser = argparse.ArgumentParser(description='电视猫 EPG 爬虫 - 全频道今日节目单')
    parser.add_argument('--output', default='EPG', help='输出目录 (默认: EPG)')
    parser.add_argument('--epg-data', default='epg_data.json', help='频道名称映射文件 (默认: epg_data.json)')
    parser.add_argument('--channels', nargs='+', help='仅抓取指定分组 (如 CCTV satellite CCTVPAYFEE)')
    args = parser.parse_args()

    # epg_data.json 路径：先看脚本同目录，再看当前目录
    script_dir = Path(__file__).parent
    epg_data_path = args.epg_data
    if not os.path.exists(epg_data_path):
        candidate = script_dir / args.epg_data
        if candidate.exists():
            epg_data_path = str(candidate)
        else:
            print(f"  警告: 未找到 {args.epg_data}，将使用 tvmao 原始频道名")
            epg_data_path = None

    crawler = TvmaoEPGCrawler(epg_data_path=epg_data_path, output_dir=args.output)
    crawler.run(filter_groups=args.channels)


if __name__ == '__main__':
    main()
