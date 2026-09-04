#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双源 EPG 爬虫 - API优先 + 电视猫补充
================================================
数据源优先级：
  1. GITV API (http://111.20.43.95:29010) — 频道列表 + 当前节目
  2. 电视猫 (https://www.tvmao.com) — 补充完整今日节目单

输出 XMLTV 格式（.xml + .xml.gz），匹配 辽宁epg格式：
  - 每个频道根据 epg_data.json 生成多个 display-name
  - 时间格式：YYYYMMDDHHMMSS +0800

用法：
  python tvmao_epg.py                    # 全部频道（API优先+TVMAO补充）
  python tvmao_epg.py --output EPG       # 指定输出目录
  python tvmao_epg.py --no-api           # 仅用电视猫
  python tvmao_epg.py --no-tvmao         # 仅用API
"""

import argparse
import gzip
import io
import json
import os
import re
import sys
import time
from threading import Lock

if sys.stdout and sys.stdout.encoding and sys.stdout.encoding.lower() in ('gbk', 'cp936', 'gb2312'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# ─────────────────────────────── 配置 ───────────────────────────────
GITV_API_URL = "http://111.20.43.95:29010/tagNewestEpgList/js_CMCC_cp/1/100/0.json"
TVMAO_BASE_URL = "https://www.tvmao.com"

# 电视猫频道分组
GROUP_DISCOVERY = {
    'CCTV':       '/program/CCTV',
    'CCTVPAYFEE': '/program/CCTVPAYFEE',
    'CETV':       '/program/CETV',
    'CHC':        '/program/CHC',
    'BAMC':       '/program/BAMC',
    'HSCM':       '/program/HSCM',
    'BTV':        '/program/BTV',
}

PROVINCE_GROUPS = {
    'LNTV':  '/program/LNTV',
    'SDTV':  '/program/SDTV',
    'HNTV':  '/program/HNTV',
    'HUBEI': '/program/HUBEI',
    'HLJTV': '/program/HLJTV',
    'JILIN': '/program/JILIN',
    'SCTV':  '/program/SCTV',
    'TJTV':  '/program/TJTV',
    'HEBEI': '/program/HEBEI',
    'AHTV':  '/program/AHTV',
    'JSTV':  '/program/JSTV',
    'ZJTV':  '/program/ZJTV',
    'GDTV':  '/program/GDTV',
    'HUNANTV': '/program/HUNANTV',
    'FJTV':  '/program/FJTV',
    'JXTV':  '/program/JXTV',
    'NMGTV': '/program/NMGTV',
    'XJTV':  '/program/XJTV',
    'XIZANGTV': '/program/XIZANGTV',
    'QHTV':  '/program/QHTV',
    'NXTV':  '/program/NXTV',
    'GUIZOUTV': '/program/GUIZOUTV',
    'YNTV':  '/program/YNTV',
    'SXTV':  '/program/SXTV',
    'SHXITV': '/program/SHXITV',
    'CCQTV': '/program/CCQTV',
    'GSTV':  '/program/GSTV',
    'TCTC':  '/program/TCTC',
    'GUANXI': '/program/GUANXI',
    'DFMV':  '/program/DFMV',
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


def normalize_name(name):
    """统一频道名用于匹配：去空格、括号、横杠"""
    if not name:
        return ""
    return re.sub(r'[\s（）()\[\]【】\-\-]+', '', name.strip())


def name_contains(a, b):
    """检查两个归一化名称是否互相包含"""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return False
    return na in nb or nb in na


# ─────────────────────────── 频道名称映射 ───────────────────────────
class ChannelNameMapper:
    def __init__(self, epg_data_path):
        self.epg_map = {}
        self.name_to_epgid = {}
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

    def match_channel(self, channel_name):
        if not channel_name:
            return None
        clean = channel_name.strip()
        if clean in self.name_to_epgid:
            return self.name_to_epgid[clean], clean
        simplified = normalize_name(clean)
        for name, epgid in self.name_to_epgid.items():
            if normalize_name(name) == simplified:
                return epgid, name
        for name, epgid in self.name_to_epgid.items():
            if clean in name or name in clean:
                return epgid, name
        return None

    def get_aliases(self, epgid):
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


def fetch_json(session, url, retries=3):
    for attempt in range(retries):
        try:
            resp = session.get(url, timeout=30, allow_redirects=True)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            if attempt < retries - 1:
                time.sleep(0.5 * (attempt + 1))
    return None


# ─────────────────────────── GITV API 数据源 ───────────────────────────
def fetch_gitv_api(session):
    """从 GITV API 获取频道列表和当前节目"""
    print("\n=== [API] 获取 GITV 频道列表 ===")
    data = fetch_json(session, GITV_API_URL)
    if not data or data.get('code') != 'A000000':
        print("  ✗ API 请求失败")
        return []

    channels = data.get('data', [])
    print(f"  ✓ 获取到 {len(channels)} 个频道")

    results = []
    for ch in channels:
        chn_name = ch.get('chnName', '').strip()
        chn_alias = ch.get('chnAlias', '').strip()
        chn_un_code = ch.get('chnunCode', '').strip()
        title = ch.get('title', '').strip()
        start_ms = ch.get('startTime')
        end_ms = ch.get('endTime')
        tag = ch.get('tag', '').strip()

        if not chn_name:
            continue

        # 转换时间
        start_dt = None
        end_dt = None
        if start_ms and isinstance(start_ms, (int, float)):
            start_dt = datetime.fromtimestamp(start_ms / 1000)
        if end_ms and isinstance(end_ms, (int, float)):
            end_dt = datetime.fromtimestamp(end_ms / 1000)

        results.append({
            'chnName': chn_name,
            'chnAlias': chn_alias,
            'chnunCode': chn_un_code,
            'chnCode': ch.get('chnCode', ''),
            'chnTypeId': ch.get('chnTypeId'),
            'chnNum': ch.get('chnNum'),
            'chnDefinition': ch.get('chnDefinition'),
            'title': title,
            'startTime': start_dt,
            'endTime': end_dt,
            'tag': tag,
            'epgStatus': ch.get('epgStatus'),
        })

    return results


# ─────────────────────────── 电视猫数据源 ───────────────────────────
def discover_today_weekday(session):
    url = f"{TVMAO_BASE_URL}/program/CCTV"
    html = fetch_page(session, url)
    if not html:
        return datetime.now().isoweekday(), datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    soup = BeautifulSoup(html, 'lxml')
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
    html = fetch_page(session, TVMAO_BASE_URL + discovery_url)
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    channels = []
    seen = set()
    for link in soup.select('a.black_link'):
        href = link.get('href', '')
        name = link.get_text(strip=True)
        if not href or not name:
            continue
        if not re.search(r'-w\d+\.html', href):
            continue
        prefix = re.sub(r'-w\d+\.html$', '', href)
        if prefix in seen:
            continue
        seen.add(prefix)
        channels.append({'url_prefix': prefix, 'name': name})
    return channels


def discover_all_tvmao_channels(session):
    all_channels = {}
    print("\n=== [TVMAO] 发现频道列表 ===")

    print("  正在发现 CCTV 频道...")
    for ch in discover_channels_from_page(session, GROUP_DISCOVERY['CCTV']):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  CCTV: {len([c for c in all_channels if '/CCTV-' in c])} 频道")

    print("  正在发现 卫视 频道...")
    for ch in discover_channels_from_page(session, '/program_satellite/AHTV1-w5.html'):
        all_channels[ch['url_prefix']] = ch['name']
    print(f"  卫视: {len([c for c in all_channels if '/program_satellite/' in c])} 频道")

    for grp_name, grp_url in GROUP_DISCOVERY.items():
        if grp_name == 'CCTV':
            continue
        print(f"  正在发现 {grp_name} 频道...")
        before = len(all_channels)
        for ch in discover_channels_from_page(session, grp_url):
            if ch['url_prefix'] not in all_channels:
                all_channels[ch['url_prefix']] = ch['name']
        print(f"  {grp_name}: +{len(all_channels) - before} 频道")

    for prov_name, prov_url in PROVINCE_GROUPS.items():
        print(f"  正在发现 {prov_name} 频道...")
        before = len(all_channels)
        for ch in discover_channels_from_page(session, prov_url):
            if ch['url_prefix'] not in all_channels:
                all_channels[ch['url_prefix']] = ch['name']
        print(f"  {prov_name}: +{len(all_channels) - before} 频道")

    print(f"\n  ✓ 共发现 {len(all_channels)} 个频道")
    return all_channels


def parse_channel_epg(html, today_w, today_date):
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    programs = []
    pgrow = soup.find('ul', id='pgrow')
    if not pgrow:
        return []

    target_date = today_date
    current_hour = 0
    for li in pgrow.find_all('li', recursive=False):
        if li.get('id') in ('noon', 'night'):
            continue
        over_hide = li.find('div', class_='over_hide')
        if not over_hide:
            continue
        time_span = over_hide.find('span', class_=re.compile(r'^(am|pm)$'))
        if not time_span:
            continue
        time_text = time_span.get_text(strip=True)
        m = re.match(r'(\d{1,2}):(\d{2})', time_text)
        if not m:
            continue
        hour, minute = int(m.group(1)), int(m.group(2))
        p_show = over_hide.find('span', class_='p_show')
        if not p_show:
            continue
        title = p_show.get_text(strip=True)
        if not title:
            continue
        if hour < current_hour:
            target_date = target_date + timedelta(days=1)
        current_hour = hour
        start = target_date.replace(hour=hour, minute=minute, second=0, microsecond=0)
        programs.append({'title': title, 'start': start})

    for i in range(len(programs)):
        if i + 1 < len(programs):
            programs[i]['stop'] = programs[i + 1]['start']
        else:
            if programs[i]['start'].day != today_date.day:
                programs[i]['stop'] = programs[i]['start'].replace(hour=6, minute=0, second=0)
            else:
                programs[i]['stop'] = programs[i]['start'].replace(hour=23, minute=59, second=59)

    return programs


# ─────────────────────────── 频道名匹配 ───────────────────────────
def match_api_to_tvmao(api_channels, tvmao_channels):
    """将 API 频道匹配到电视猫频道，返回 {api_index: tvmao_prefix}"""
    # 构建电视猫名称索引
    tvmao_name_map = {}
    for prefix, name in tvmao_channels.items():
        tvmao_name_map[name] = prefix
        # 也用简化名索引
        simplified = normalize_name(name)
        if simplified not in tvmao_name_map:
            tvmao_name_map[simplified] = prefix

    matches = {}
    for i, ch in enumerate(api_channels):
        chn_name = ch['chnName']
        chn_alias = ch['chnAlias']

        # 1. 精确匹配 chnName
        if chn_name in tvmao_name_map:
            matches[i] = tvmao_name_map[chn_name]
            continue

        # 2. 精确匹配 chnAlias
        if chn_alias and chn_alias in tvmao_name_map:
            matches[i] = tvmao_name_map[chn_alias]
            continue

        # 3. 归一化匹配
        api_simplified = normalize_name(chn_name)
        for tvmao_name, prefix in tvmao_channels.items():
            if normalize_name(tvmao_name) == api_simplified:
                matches[i] = prefix
                break
        if i in matches:
            continue

        # 4. 模糊匹配：互相包含
        for tvmao_name, prefix in tvmao_channels.items():
            if name_contains(chn_name, tvmao_name):
                matches[i] = prefix
                break

    return matches


# ─────────────────────────── 主流程 ───────────────────────────
class HybridEPGCrawler:
    def __init__(self, epg_data_path='epg_data.json', output_dir='EPG',
                 use_api=True, use_tvmao=True):
        self.output_dir = output_dir
        self.use_api = use_api
        self.use_tvmao = use_tvmao
        self.session = create_session()
        self.mapper = ChannelNameMapper(epg_data_path)
        self.today_w = None
        self.today_date = None
        self.lock = Lock()
        self.all_programs = []
        # epgid -> {aliases, source}
        self.channel_defs = {}

    def run(self):
        os.makedirs(self.output_dir, exist_ok=True)
        print("=" * 60)
        print("双源 EPG 爬虫启动 (API优先 + TVMAO补充)")
        print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"数据源: {'API' if self.use_api else ''}"
              f"{' + ' if self.use_api and self.use_tvmao else ''}"
              f"{'TVMAO' if self.use_tvmao else ''}")
        print("=" * 60)

        # ── 1. 检测今天星期 ──
        if self.use_tvmao:
            print("\n=== 检测今天星期 ===")
            self.today_w, self.today_date = discover_today_weekday(self.session)
            print(f"  今天: w{self.today_w} ({self.today_date.strftime('%Y-%m-%d')})")
        else:
            self.today_w = datetime.now().isoweekday()
            self.today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # ── 2. 获取 API 频道列表 ──
        api_channels = []
        if self.use_api:
            api_channels = fetch_gitv_api(self.session)

        # ── 3. 获取 TVMAO 频道列表 ──
        tvmao_channels = {}
        if self.use_tvmao:
            tvmao_channels = discover_all_tvmao_channels(self.session)

        # ── 4. 匹配 API ↔ TVMAO ──
        api_to_tvmao = {}
        if api_channels and tvmao_channels:
            print("\n=== 匹配 API ↔ TVMAO 频道 ===")
            api_to_tvmao = match_api_to_tvmao(api_channels, tvmao_channels)
            matched = len(api_to_tvmao)
            api_only = len(api_channels) - matched
            tvmao_only = len(tvmao_channels) - len(set(api_to_tvmao.values()))
            print(f"  双源匹配: {matched} 频道")
            print(f"  仅API: {api_only} 频道")
            print(f"  仅TVMAO: {tvmao_only} 频道")

        # ── 5. 匹配频道名称到 epgid ──
        print("\n=== 匹配频道名称到 epgid ===")
        matched_count = 0

        # 5a. API 频道
        for i, ch in enumerate(api_channels):
            chn_name = ch['chnName']
            chn_alias = ch['chnAlias']
            # 优先用 chnAlias 匹配，再用 chnName
            result = self.mapper.match_channel(chn_alias) or self.mapper.match_channel(chn_name)
            if result:
                epgid, matched_name = result
                aliases = self.mapper.get_aliases(epgid)
                self.channel_defs[epgid] = {'aliases': aliases, 'source': 'api'}
                matched_count += 1
            else:
                epgid = chn_name
                aliases = [chn_name]
                if chn_alias and chn_alias != chn_name:
                    aliases = [chn_name, chn_alias]
                self.channel_defs[epgid] = {'aliases': aliases, 'source': 'api'}

        # 5b. TVMAO-only 频道（不与 API 重叠的）
        if tvmao_channels:
            api_matched_prefixes = set(api_to_tvmao.values())
            for prefix, tvmao_name in tvmao_channels.items():
                if prefix in api_matched_prefixes:
                    continue
                result = self.mapper.match_channel(tvmao_name)
                if result:
                    epgid, matched_name = result
                    aliases = self.mapper.get_aliases(epgid)
                    if epgid not in self.channel_defs:
                        self.channel_defs[epgid] = {'aliases': aliases, 'source': 'tvmao'}
                    matched_count += 1
                else:
                    if tvmao_name not in self.channel_defs:
                        self.channel_defs[tvmao_name] = {'aliases': [tvmao_name], 'source': 'tvmao'}

        print(f"  匹配: {matched_count} 频道")

        # ── 6. 抓取节目数据 ──
        if self.use_api:
            self._collect_api_programs(api_channels, api_to_tvmao)

        if self.use_tvmao:
            self._crawl_tvmao_programs(tvmao_channels, api_to_tvmao)

        # ── 7. 去重 ──
        print("\n=== 去重 ===")
        self._deduplicate()

        # ── 8. 生成 XMLTV ──
        print("\n=== 生成 XMLTV ===")
        self._generate_xmltv()

        print("\n" + "=" * 60)
        print("全部完成！")
        print("=" * 60)

    def _collect_api_programs(self, api_channels, api_to_tvmao):
        """从 API 收集当前节目（API优先，匹配到TVMAO的用TVMAO的epgid）"""
        print(f"\n=== [API] 收集当前节目 ({len(api_channels)} 频道) ===")
        count = 0
        for i, ch in enumerate(api_channels):
            if not ch.get('title') or not ch.get('startTime'):
                continue

            # 确定 epgid
            epgid = ch['chnName']
            if i in api_to_tvmao:
                # 匹配到 TVMAO，用 epg_data 的 epgid
                prefix = api_to_tvmao[i]
                result = self.mapper.match_channel(ch['chnAlias']) or self.mapper.match_channel(ch['chnName'])
                if result:
                    epgid = result[0]

            start_dt = ch['startTime']
            end_dt = ch['endTime']
            if not end_dt:
                end_dt = start_dt + timedelta(hours=1)

            self.all_programs.append({
                'channel_id': epgid,
                'title': ch['title'],
                'start': start_dt,
                'stop': end_dt,
                'source': 'api',
            })
            count += 1

        print(f"  ✓ API 节目: {count} 条")

    def _crawl_tvmao_programs(self, tvmao_channels, api_to_tvmao):
        """从 TVMAO 抓取完整今日节目单（跳过已有 API 数据的频道）"""
        # 找出已有 API 节目的 epgid
        api_epgids = set()
        for p in self.all_programs:
            if p.get('source') == 'api':
                api_epgids.add(p['channel_id'])

        # 只抓取没有 API 数据的 TVMAO 频道
        tvmao_to_fetch = {}
        for prefix, name in tvmao_channels.items():
            # 找这个 prefix 对应的 epgid
            result = self.mapper.match_channel(name)
            epgid = result[0] if result else name
            if epgid not in api_epgids:
                tvmao_to_fetch[prefix] = name

        if not tvmao_to_fetch:
            print("\n=== [TVMAO] 所有频道已有 API 数据，跳过 ===")
            return

        print(f"\n=== [TVMAO] 抓取今日 EPG ({len(tvmao_to_fetch)} 频道，跳过 {len(tvmao_channels) - len(tvmao_to_fetch)} 个已有API数据) ===")
        progress_lock = Lock()
        done_count = [0]
        total = len(tvmao_to_fetch)

        def fetch_one(prefix, name):
            url = f"{TVMAO_BASE_URL}{prefix}-w{self.today_w}.html"
            html = fetch_page(self.session, url)
            if html:
                programs = parse_channel_epg(html, self.today_w, self.today_date)
                result = self.mapper.match_channel(name)
                epgid = result[0] if result else name
                with self.lock:
                    for p in programs:
                        self.all_programs.append({
                            'channel_id': epgid,
                            'title': p['title'],
                            'start': p['start'],
                            'stop': p['stop'],
                            'source': 'tvmao',
                        })
            with progress_lock:
                done_count[0] += 1
                if done_count[0] % 20 == 0 or done_count[0] == total:
                    print(f"  进度: {done_count[0]}/{total} ({done_count[0]*100//total}%)")

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(fetch_one, prefix, name) for prefix, name in tvmao_to_fetch.items()]
            for f in as_completed(futures):
                try:
                    f.result(timeout=30)
                except Exception:
                    pass

        tvmao_count = sum(1 for p in self.all_programs if p.get('source') == 'tvmao')
        print(f"  ✓ TVMAO 节目: {tvmao_count} 条")

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
        root = ET.Element('tv')
        root.set('generator-info-name', 'GITV_TVMAO_EPG')
        root.set('generator-info-url', 'https://www.tvmao.com')

        # 收集所有有节目的 epgid
        epgids_with_programs = set(p['channel_id'] for p in self.all_programs)

        # 为每个 epgid 生成 channel 定义
        for epgid in sorted(epgids_with_programs):
            aliases = self.mapper.get_aliases(epgid)
            if not aliases or aliases == [epgid]:
                # 没有 epg_data 映射，用 channel_defs 中的别名
                ch_def = self.channel_defs.get(epgid, {})
                aliases = ch_def.get('aliases', [epgid])
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

        xml_path = os.path.join(self.output_dir, 'epg.xml')
        gz_path = os.path.join(self.output_dir, 'epg.xml.gz')
        write_xml_and_gz(root, xml_path, gz_path)

        channel_count = len(epgids_with_programs)
        api_count = sum(1 for p in self.all_programs if p.get('source') == 'api')
        tvmao_count = sum(1 for p in self.all_programs if p.get('source') == 'tvmao')
        print(f"\n  频道数: {channel_count}")
        print(f"  节目数: {len(self.all_programs)} (API: {api_count}, TVMAO: {tvmao_count})")


# ─────────────────────────── 入口 ───────────────────────────
def main():
    parser = argparse.ArgumentParser(description='双源 EPG 爬虫 - API优先 + TVMAO补充')
    parser.add_argument('--output', default='EPG', help='输出目录 (默认: EPG)')
    parser.add_argument('--epg-data', default='epg_data.json', help='频道名称映射文件')
    parser.add_argument('--no-api', action='store_true', help='不使用 GITV API')
    parser.add_argument('--no-tvmao', action='store_true', help='不使用电视猫')
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    epg_data_path = args.epg_data
    if not os.path.exists(epg_data_path):
        candidate = script_dir / args.epg_data
        if candidate.exists():
            epg_data_path = str(candidate)
        else:
            print(f"  警告: 未找到 {args.epg_data}")
            epg_data_path = None

    use_api = not args.no_api
    use_tvmao = not args.no_tvmao

    if not use_api and not use_tvmao:
        print("错误: 至少启用一个数据源")
        sys.exit(1)

    crawler = HybridEPGCrawler(
        epg_data_path=epg_data_path,
        output_dir=args.output,
        use_api=use_api,
        use_tvmao=use_tvmao,
    )
    crawler.run()


if __name__ == '__main__':
    main()
