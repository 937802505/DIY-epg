#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
四川 EPG 聚合脚本（独立版）
================================================================
整合 SD-EPG 四个抓取源为单一独立脚本：
  1. TVMAO 电视猫        —— 抓取四川各台今天节目单
  2. TVSOU 搜视网        —— 抓取指定四川频道（需填 tvsou_id，可留空跳过）
  3. SDGD 山东广电 API   —— 可选，需要 SDGD_TOKEN（环境变量/token_health.log/flows.json）
  4. FalseEPG 兜底       —— 为无真实EPG的四川本地频道生成昨天/今天/明天假节目单

输出：
  scepg.xml        未压缩 XMLTV（无 DOCTYPE，channel 多别名，+0800 时间）
  scepg.xml.gz     同内容的 gzip 压缩版

省份配置：四川 TVMAO 地区代码 = SCTV(四川) / CHENGDU(成都) / SCYBTV(宜宾) / SCMYTV(绵阳) / satellite(卫视)

用法：
  python scepg.py                 # 全部源运行（SDGD 无 token 时自动跳过）
  python scepg.py --no-tvmao      # 跳过 TVMAO
  python scepg.py --no-false      # 跳过兜底假EPG
  python scepg.py --output EPG    # 指定输出目录
"""

import argparse
import gzip
import io
import json
import os
import re
import sys
import threading
import time

if sys.stdout and sys.stdout.encoding and sys.stdout.encoding.lower() in ('gbk', 'cp936', 'gb2312'):
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET

# ─────────────────────────────── 配置 ───────────────────────────────
# TVMAO 抓取源列表（全套：卫视 + 央视 + 四川各台 + 付费/传媒集团 + 四川全部市州）
# 说明：
#   satellite   = 卫星频道（全国卫视）
#   CCTV        = 央视（CCTV-1~17 等 26 个台）
#   CCTVPAYFEE  = 央视付费（风云足球、第一剧场等 34 台）—— 中数传媒
#   CETV        = 中国教育电视台
#   CHC         = CHC 华诚付费（CHC动作/家庭影院/影迷电影）
#   BAMC        = 北广传媒集团
#   HSCM        = 华数传媒（求索纪录、HD探索纪录等）
#   SHHAI       = 上海（东方卫视等）
#   SCTV        = 四川台（四川卫视+四川本地频道）
#   其余        = 四川全部市州电视台（成都/攀枝花/宜宾/自贡/南充/德阳/绵阳/广元/乐山/遂宁/资阳/眉山/雅安/内江/凉山/泸州 + 星元传媒）
TVMAO_PROVINCES = [
    "satellite", "CCTV", "CCTVPAYFEE", "CETV", "CHC", "BAMC", "HSCM", "SHHAI",
    "SCTV", "CHENGDU",
    "SCPZHTV", "SCYBTV", "SCZGTV", "SCNCTV", "SCDYTV", "SCMYTV", "SCGYTV",
    "LESHANTV", "XYCM", "SUININGTV", "ZIYANGTV", "MSTV", "YAANTV", "NEIJTV",
    "LIANGSTV", "LUZHOUTV",
]

# TVSOU 四川频道列表。格式: {"id": "用于XML的ID", "name": "频道名", "tvsou_id": "搜视网频道码"}
# 留空 [] 则跳过 TVSOU 抓取。如需使用请在 tvsou 官网频道页获取频道码后填入。
TVSOU_CHANNELS = []

# FalseEPG 兜底频道（四川本地台，无真实EPG时生成假节目单）
FALSE_EPG_CHANNELS = [
    {"id": "四川卫视", "name": "四川卫视", "programs": ["四川新闻联播", "四川天气预报", "黄金剧场"]},
    {"id": "四川文化旅游", "name": "四川文化旅游频道", "programs": ["文旅四川", "精彩栏目"]},
    {"id": "四川经济", "name": "四川经济频道", "programs": ["经济四川", "专题报道"]},
    {"id": "四川新闻", "name": "四川新闻频道", "programs": ["新闻现场", "今日四川"]},
    {"id": "四川影视文艺", "name": "四川影视文艺频道", "programs": ["影视欣赏", "文艺荟萃"]},
    {"id": "四川星空购物", "name": "四川星空购物频道", "programs": ["星空购物", "欢乐购物"]},
    {"id": "四川妇女儿童", "name": "四川妇女儿童频道", "programs": ["妇女儿童", "亲子时光"]},
    {"id": "四川乡村", "name": "四川乡村频道", "programs": ["乡村风景", "三农报道"]},
    {"id": "峨眉电影", "name": "峨眉电影频道", "programs": ["经典电影", "佳片有约"]},
    {"id": "成都新闻综合", "name": "成都新闻综合频道", "programs": ["成都新闻", "蓉城热线"]},
    {"id": "成都经济资讯", "name": "成都经济资讯频道", "programs": ["蓉城经济", "资讯快递"]},
    {"id": "成都都市生活", "name": "成都都市生活频道", "programs": ["都市生活", "蓉城风采"]},
    {"id": "成都影视文艺", "name": "成都影视文艺频道", "programs": ["影视剧场", "文艺节目"]},
    {"id": "成都公共", "name": "成都公共频道", "programs": ["公共新闻", "民生服务"]},
    {"id": "成都少儿", "name": "成都少儿频道", "programs": ["少儿动画", "儿童节目"]},
    {"id": "宜宾新闻综合", "name": "宜宾新闻综合频道", "programs": ["宜宾新闻", "三江报道"]},
    {"id": "绵阳新闻综合", "name": "绵阳新闻综合频道", "programs": ["绵阳新闻", "绵州视线"]},
    {"id": "绵阳影视科技", "name": "绵阳影视科技频道", "programs": ["影视剧场", "科技之窗"]},
]

# ─────────────────────────── 共享工具 ───────────────────────────
ILLEGAL_XML_CHARS_RE = re.compile(
    u'[\x00-\x08\x0b\x0c\x0e-\x1F\x7F-\x84\x86-\x9f'
    u'\ud800-\udfff\ufdd0-\ufdef\ufffe\uffff]'
)


def clean_xml_text(text):
    """清理字符串中的非法 XML 控制字符"""
    if not text:
        return ""
    return ILLEGAL_XML_CHARS_RE.sub('', text).strip()


def indent_xml(elem, level=0):
    """格式化 XML 缩进（兼容 Python 3.9 以下）"""
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
    """写出对齐 sggc.xml 格式：无 DOCTYPE、标准 xml 声明 + 缩进正文"""
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


# ─────────────────────────── 1. TVMAO 电视猫 ───────────────────────────
class TvmaoEPGCrawler:
    """电视猫 EPG 爬虫（对齐 sggc.xml 标准格式）"""

    def __init__(self, province_id='SCTV', output_dir='EPG'):
        self.province_id = province_id
        self.output_dir = output_dir
        self.base_url = "https://www.tvmao.com/program/duration"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.tvmao.com/',
        })
        self.hour_blocks = list(range(0, 24, 2))
        self.channels = {}   # ch_id -> (ch_name, aliases)
        self.programs = []
        self.today_w = None
        self.today_date = None
        self.lock = threading.Lock()

    def detect_today_w(self):
        url = f"{self.base_url}/{self.province_id}"
        print(f"  [{self.province_id}] 检测今天星期...")
        try:
            response = self.session.get(url, timeout=20, allow_redirects=True)
            response.raise_for_status()
            final_url = response.url
            match = re.search(r'w(\d)-h\d+', final_url)
            if match:
                self.today_w = int(match.group(1))
                self.today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                print(f"  [{self.province_id}] 今天: w{self.today_w}")
                return True
            soup = BeautifulSoup(response.text, 'lxml')
            links = soup.find_all('a', href=re.compile(r'w\d-h\d+'))
            if links:
                match = re.search(r'w(\d)-h\d+', links[0]['href'])
                if match:
                    self.today_w = int(match.group(1))
                    self.today_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    return True
        except Exception as e:
            print(f"  [{self.province_id}] 检测失败: {e}")
            return False

    def get_w_and_date(self, day_offset):
        target_date = self.today_date + timedelta(days=day_offset)
        target_w = (self.today_w + day_offset - 1) % 7 + 1
        return target_w, target_date

    def fetch_page(self, week_num, hour_block):
        url = f"{self.base_url}/{self.province_id}/w{week_num}-h{hour_block}.html"
        try:
            resp = self.session.get(url, timeout=25)
            resp.raise_for_status()
            return resp.text
        except Exception:
            return None

    def _normalize_channel_id(self, href, name):
        match = re.search(r'/program/([^/]+)/([^/]+?)(?:-w\d+)?\.html', href or '')
        if match:
            code = match.group(2).upper().replace('CCTV-', 'CCTV').replace('TV1', '')
            mappings = {
                'CCTV1': "CCTV1,CCTV-1,CCTV1综合,CCTV-1综合,CCTV-1 综合,CCTV1高清,CCTV1HD,CCTV-1高清",
                'CCTV2': "CCTV2,CCTV-2,CCTV2财经,CCTV-2财经,CCTV-2 财经,CCTV2高清,CCTV2HD",
                'CCTV3': "CCTV3,CCTV-3,CCTV3综艺,CCTV-3综艺,CCTV-3 综艺,CCTV3高清,CCTV3HD",
                'CCTV4': "CCTV4,CCTV-4,CCTV4中文国际,CCTV-4中文国际,CCTV-4 中文国际,CCTV4高清,CCTV4HD",
                'CCTV5': "CCTV5,CCTV-5,CCTV5体育,CCTV-5体育,CCTV-5 体育,CCTV5高清,CCTV5HD",
                'CCTV5+': "CCTV5+,CCTV-5+,CCTV5+体育赛事,CCTV-5+体育赛事,CCTV5+高清,CCTV5+HD",
                'CCTV6': "CCTV6,CCTV-6,CCTV6电影,CCTV-6电影,CCTV-6 电影,CCTV6高清,CCTV6HD",
                'CCTV7': "CCTV7,CCTV-7,CCTV7国防军事,CCTV-7国防军事,CCTV7高清,CCTV7HD",
                'CCTV8': "CCTV8,CCTV-8,CCTV8电视剧,CCTV-8电视剧,CCTV-8 电视剧,CCTV8高清,CCTV8HD",
                'CCTV9': "CCTV9,CCTV-9,CCTV9纪录,CCTV-9纪录,CCTV-9 纪录,CCTV9高清,CCTV9HD",
                'CCTV10': "CCTV10,CCTV-10,CCTV10科教,CCTV-10科教,CCTV-10 科教,CCTV10高清,CCTV10HD",
                'CCTV11': "CCTV11,CCTV-11,CCTV11戏曲,CCTV-11戏曲,CCTV-11 戏曲,CCTV11高清,CCTV11HD",
                'CCTV12': "CCTV12,CCTV-12,CCTV12社会与法,CCTV-12社会与法,CCTV12高清,CCTV12HD",
                'CCTV13': "CCTV13,CCTV-13,CCTV13新闻,CCTV-13新闻,CCTV-13 新闻,CCTV13高清,CCTV13HD",
                'CCTV14': "CCTV14,CCTV-14,CCTV14少儿,CCTV-14少儿,CCTV-14 少儿,CCTV14高清,CCTV14HD",
                'CCTV15': "CCTV15,CCTV-15,CCTV15音乐,CCTV-15音乐,CCTV15高清,CCTV15HD",
                'CCTV16': "CCTV16,CCTV-164k,CCTV16奥林匹克,CCTV-16奥林匹克,CCTV16高清,CCTV16HD,CCTV164k",
                'CCTV17': "CCTV17,CCTV-17,CCTV17农业农村,CCTV-17农业农村,CCTV17高清,CCTV17HD",
                'CCTV4K': "CCTV4K,CCTV4K超高清,CCTV-4K超高清",
                '四川卫视': "四川卫视,SCTV-1,SCTV1,四川卫视频道,四川卫视[超清HDR],四川卫视HD",
                '四川新闻': "四川新闻,SCTV新闻,SCTV-4新闻,四川新闻频道,四川新闻高清,SCTV4",
                '四川经济': "四川经济,SCTV经济,SCTV-2经济,四川经济频道,四川经济高清,SCTV2",
                '四川影视文艺': "四川影视文艺,SCTV影视文艺,SCTV-5影视,四川影视,四川影视高清,SCTV5",
                '四川文化旅游': "四川文化旅游,SCTV文旅,SCTV-3文旅,四川文旅,四川文旅高清,四川文化,SCTV3",
                '四川科教': "四川科教,SCTV-8,四川科教频道,四川科教高清,SCTV8",
                '四川妇女儿童': "四川妇女儿童,SCTV妇女儿童,SCTV-7妇儿,四川妇儿,四川妇儿高清,SCTV7",
                '四川乡村': "四川乡村,四川乡村频道,SCTV乡村,SCTV-9乡村,四川公共乡村,四川乡村高清",
                '四川星空购物': "四川星空购物,SCTV星空购物,SCTV-6购物,四川购物,四川购物高清,SCTV6",
                '峨眉电影': "四川峨眉电影,四川峨眉电影频道,峨眉电影",
                '康巴卫视': "康巴卫视,SCTV11康巴卫视,四川康巴卫视,康巴藏语综合",
                '成都新闻综合': "成都新闻综合,CDTV1,CDTV-1综合,成都综合,成都综合高清,成都新闻,CDTV1综合",
                '成都经济资讯': "成都经济资讯,CDTV2,CDTV-2经济,成都经济,成都经济资讯,CDTV2经济",
                '成都都市生活': "成都都市生活,CDTV3,CDTV-3生活,成都生活,成都生活高清,成都都市,CDTV3生活",
                '成都影视文艺': "成都影视文艺,CDTV4,CDTV-4影视,成都影视,成都影视文艺,CDTV4影视",
                '成都公共': "成都公共,CDTV5,CDTV-5公共,成都公共频道,成都公共高清,CDTV5公共",
                '宜宾新闻综合': "宜宾新闻综合,宜宾新闻,宜宾综合,宜宾新闻频道,宜宾电视台新闻",
                '绵阳新闻综合': "绵阳新闻综合,绵阳新闻,绵阳综合,绵阳新闻频道",
                '绵阳影视科技': "绵阳影视科技,绵阳影视,绵阳科技频道",
                '攀枝花新闻综合': "攀枝花新闻综合,攀枝花综合,攀枝花新闻,攀枝花电视台",
                '攀枝花文旅生活': "攀枝花文旅生活,攀枝花文旅,攀枝花生活频道",
                '自贡综合': "自贡综合,自贡新闻综合,自贡电视台综合",
                '自贡公共': "自贡公共,自贡公共频道,自贡电视台公共",
                '南充综合': "南充综合,南充新闻综合,南充电视台综合",
                '南充公共': "南充公共,南充公共频道,南充电视台公共",
                '南充文娱': "南充文娱,南充文娱频道,南充电视台文娱",
                '德阳都市': "德阳都市,德阳都市频道,德阳电视台都市",
                '德阳财富': "德阳财富,德阳财富频道,德阳电视台财富",
                '广元综合': "广元综合,广元新闻综合,广元电视台综合",
                '广元公共': "广元公共,广元公共频道,广元电视台公共",
                '乐山新闻综合': "乐山新闻综合,乐山综合,乐山新闻,乐山电视台",
                '乐山文旅生活': "乐山文旅生活,乐山文旅,乐山生活频道",
                '遂宁综合': "遂宁综合,遂宁新闻综合,遂宁电视台综合",
                '遂宁公共': "遂宁公共,遂宁公共频道,遂宁电视台公共",
                '遂宁互动': "遂宁互动,遂宁互动频道",
                '资阳新闻综合': "资阳新闻综合,资阳综合,资阳新闻,资阳电视台",
                '眉山综合': "眉山综合,眉山新闻综合,眉山电视台综合",
                '眉山影视剧': "眉山影视剧,眉山影视,眉山影视剧频道",
                '眉山文旅乡村': "眉山文旅乡村,眉山文旅,眉山乡村频道",
                '雅安新闻综合': "雅安新闻综合,雅安综合,雅安新闻,雅安电视台",
                '雅安经视': "雅安经视,雅安经济频道,雅安经视频道",
                '雅安公共': "雅安公共,雅安公共频道,雅安电视台公共",
                '内江城市公共': "内江城市公共,内江公共,内江公共频道",
                '内江影视文艺': "内江影视文艺,内江影视,内江影视文艺频道",
                '凉山新闻': "凉山新闻,凉山新闻频道,凉山电视台新闻",
                '凉山公共': "凉山公共,凉山公共频道,凉山电视台公共",
                '泸州新闻': "泸州新闻,泸州新闻频道,泸州电视台新闻",
                '风云足球': "风云足球,CCTV风云足球,央视风云足球,风云足球HD",
                '风云音乐': "风云音乐,CCTV风云音乐,央视风云音乐,风云音乐HD",
                '第一剧场': "第一剧场,CCTV第一剧场,央视第一剧场,第一剧场HD",
                'CHC动作电影': "CHC动作电影,动作电影,华诚动作电影",
                'CHC家庭影院': "CHC家庭影院,家庭影院,华诚家庭影院",
                'CHC影迷电影': "CHC影迷电影,影迷电影,华诚影迷电影",
                'HD探索纪录': "HD探索纪录,探索纪录,华数探索纪录",
                'HD欧美影院': "HD欧美影院,欧美影院,华数欧美影院",
                '求索纪录': "求索纪录,华数求索纪录,求索纪录HD",
                '优优宝贝': "优优宝贝,北广优优宝贝",
                '四海钓鱼': "四海钓鱼,北广四海钓鱼,四海钓鱼HD",
                '中国教育': "中国教育,中国教育1,CETV1,CETV-1,CETV1HD",
                '中教二台': "中教二台,中国教育2,CETV2,CETV-2",
                '中教三台': "中教三台,中国教育3,CETV3,CETV-3",
                '中教四台': "中教四台,中国教育4,CETV4,CETV-4,CETV4HD",
                '东方卫视': "东方卫视,东方卫视[超清HDR],东方卫视咪咕,东方卫视HD",
                '湖南卫视': "湖南卫视,湖南卫视[超清HDR],湖南卫视咪咕,湖南卫视HD",
                '浙江卫视': "浙江卫视,浙江卫视[超清HDR],浙江卫视咪咕,浙江卫视HD",
                '江苏卫视': "江苏卫视,江苏卫视[超清HDR],江苏卫视咪咕,江苏卫视HD",
                '北京卫视': "北京卫视,北京卫视咪咕,北京卫视HD",
                '山东卫视': "山东卫视,山东卫视咪咕,山东卫视[超清HDR],山东卫视HD",
                '广东卫视': "广东卫视,广东卫视[超清HDR],广东卫视咪咕,广东卫视HD",
                '深圳卫视': "深圳卫视,深圳卫视咪咕,深圳卫视[超清HDR],深圳卫视HD",
                '安徽卫视': "安徽卫视,安徽卫视[高清],安徽卫视HD",
                '天津卫视': "天津卫视,天津卫视[高清],天津卫视HD",
                'CGTN英文': "CGTN英语,CGTN英文,CGTN,CGTN英语频道,CGTNHD",
                '凤凰中文': "凤凰中文,凤凰中文频道,凤凰卫视中文,凤凰卫视",
                '凤凰资讯': "凤凰资讯,凤凰资讯台,凤凰资讯频道,凤凰卫视资讯",
            }
            alias_str = mappings.get(code)
            if alias_str:
                aliases = [a.strip() for a in alias_str.split(',') if a.strip()]
                return code, aliases
            safe_id = re.sub(r'[^\w\-.]', '_', code)
            return safe_id, [code]

        clean_name = re.sub(r'[\s（）(）]+', '', name)
        safe_id = re.sub(r'[^\w\-.]', '_', clean_name)
        return safe_id or 'unknown', [clean_name]

    def parse_page(self, html, target_date):
        if not html:
            return
        soup = BeautifulSoup(html, 'lxml')
        tables = soup.find_all('table', class_='timetable')
        for table in tables:
            for row in table.find_all('tr'):
                ch_cell = row.find('td', class_='tdchn')
                if not ch_cell:
                    continue
                link = ch_cell.find('a', class_='black_link')
                if not link:
                    continue
                href = link.get('href', '')
                ch_name = link.get_text(strip=True)
                ch_id, aliases = self._normalize_channel_id(href, ch_name)

                with self.lock:
                    if ch_id not in self.channels:
                        self.channels[ch_id] = (ch_name, aliases)

                for cell in row.find_all('td', class_='tdpro'):
                    title_div = cell.find('div', class_='font14')
                    if not title_div:
                        continue
                    title = title_div.get_text(strip=True)
                    time_div = cell.find('div', class_='font13')
                    if not time_div:
                        continue
                    time_text = time_div.get_text(strip=True)
                    m = re.match(r'(\d{1,2}):(\d{2})-(\d{1,2}):(\d{2})', time_text)
                    if not m:
                        continue
                    sh, sm, eh, em = map(int, m.groups())
                    start_d = target_date
                    end_d = target_date + timedelta(days=1) if (eh < sh or (eh == sh and em < sm)) else target_date
                    try:
                        start = start_d.replace(hour=sh, minute=sm, second=0, microsecond=0)
                        end = end_d.replace(hour=eh, minute=em, second=0, microsecond=0)
                        with self.lock:
                            self.programs.append({
                                'channel_id': ch_id,
                                'title': title.strip(),
                                'start': start,
                                'stop': end
                            })
                    except ValueError:
                        continue

    def crawl(self):
        print(f"\n=== 开始抓取 {self.province_id} (仅今日) ===")
        if not self.detect_today_w():
            return False
        target_w, target_date = self.get_w_and_date(0)
        print(f"  [{self.province_id}] 抓取 今天 (w{target_w})")

        def fetch_hour(h):
            html = self.fetch_page(target_w, h)
            if html:
                before = len(self.programs)
                self.parse_page(html, target_date)
                return len(self.programs) - before
            return 0

        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = [executor.submit(fetch_hour, h) for h in self.hour_blocks]
            for future in as_completed(futures):
                pass
            time.sleep(0.2)
        print(f"✓ {self.province_id} 完成: {len(self.channels)} 频道, {len(self.programs)} 节目")
        return True

    def remove_duplicates(self):
        seen = set()
        unique = []
        for p in self.programs:
            key = (p['channel_id'], p['start'].strftime('%Y%m%d%H%M'))
            if key not in seen:
                seen.add(key)
                unique.append(p)
        self.programs = unique


# ─────────────────────────── 2. TVSOU 搜视网 ───────────────────────────
class TvsouEPG:
    """搜视网 EPG 爬虫（频道列表见 TVSOU_CHANNELS 配置）"""

    def __init__(self, channels, max_workers=5):
        self.base_url = "https://www.tvsou.com/epg/{channel_id}/w{weekday}"
        self.channels = channels
        self.epg_data = []
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        })

    @staticmethod
    def parse_time(time_str):
        try:
            hour, minute = map(int, time_str.split(':'))
            return hour, minute
        except Exception:
            return 0, 0

    def fetch_page(self, url):
        retries = 3
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=12)
                response.raise_for_status()
                response.encoding = 'utf-8'
                return response.text
            except requests.exceptions.RequestException:
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                return None
        return None

    def parse_programs(self, html, date):
        programs = []
        soup = BeautifulSoup(html, 'html.parser')
        day_div = soup.find('div', class_='layui-tab-item layui-show')
        if not day_div:
            return programs
        table = day_div.find('table', class_='c_table')
        if not table:
            return programs
        rows = table.find_all('tr')
        current_date = date
        for i, row in enumerate(rows):
            tds = row.find_all('td')
            if len(tds) >= 2:
                time_text = tds[0].get_text(strip=True)
                name_text = tds[1].get_text(strip=True)
                if time_text and name_text:
                    hour, minute = self.parse_time(time_text)
                    if hour < 6 and i > 0:
                        program_date = current_date + timedelta(days=1)
                    else:
                        program_date = current_date
                    date_str = program_date.strftime('%Y%m%d')
                    start_time = f"{date_str}{hour:02d}{minute:02d}00 +0800"
                    programs.append({
                        'time': (hour, minute),
                        'name': name_text,
                        'date': date_str,
                        'start': start_time,
                    })
        for i, prog in enumerate(programs):
            if i + 1 < len(programs):
                next_prog = programs[i + 1]
                end_time = f"{next_prog['date']}{next_prog['time'][0]:02d}{next_prog['time'][1]:02d}00 +0800"
            else:
                if prog['time'][0] >= 18:
                    next_date = current_date + timedelta(days=1)
                    end_time = f"{next_date.strftime('%Y%m%d')}060000 +0800"
                else:
                    end_time = f"{prog['date']}{prog['time'][0]+2:02d}{prog['time'][1]:02d}00 +0800"
            programs[i]['stop'] = end_time
        return programs

    def fetch_channel_day(self, channel_info, date):
        channel_id = channel_info['id']
        tvsou_id = channel_info['tvsou_id']
        channel_name = channel_info['name']
        weekday = date.weekday() + 1
        url = self.base_url.format(channel_id=tvsou_id, weekday=weekday)
        html = self.fetch_page(url)
        if not html:
            print(f"  {channel_name}: 获取 w{weekday} 失败")
            return []
        programs = self.parse_programs(html, date)
        result = [{
            'channel_id': channel_id,
            'start': prog['start'],
            'stop': prog['stop'],
            'title': prog['name'],
        } for prog in programs]
        print(f"  {channel_name}: {date.strftime('%Y-%m-%d')} 共{len(programs)}条")
        return result

    def fetch_channel(self, channel_info):
        all_programs = []
        today = datetime.now().date()
        for day_offset in [0, 1]:
            target_date = today + timedelta(days=day_offset)
            all_programs.extend(self.fetch_channel_day(channel_info, target_date))
        return all_programs

    def fetch_all(self):
        print(f"\n=== TVSOU 抓取 ({len(self.channels)} 频道) ===")
        all_programs = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.fetch_channel, ch) for ch in self.channels]
            for future in as_completed(futures):
                try:
                    all_programs.extend(future.result(timeout=30))
                except Exception as e:
                    print(f"  TVSOU 频道失败: {e}")
        print(f"✓ TVSOU 完成: {len(all_programs)} 节目")
        return all_programs


# ─────────────────────────── 3. SDGD 山东广电 API ───────────────────────────
SDGD_BASE_URL = "https://api-lc.sdcabletv.cn:4443"


def sdgd_load_token():
    """加载 token，优先级：1.环境变量 SDGD_TOKEN 2.token_health.log 3.flows.json"""
    env_token = os.environ.get('SDGD_TOKEN')
    if env_token:
        return env_token
    script_dir = Path(__file__).parent
    token_file = script_dir / "token_health.log"
    if token_file.exists():
        try:
            with open(token_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if 'token=' in line:
                        token = line.split('token=')[1].split('&')[0].strip()
                        if token:
                            return token
        except Exception:
            pass
    flows_file = script_dir / "flows.json"
    if flows_file.exists():
        try:
            with open(flows_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for flow in data:
                    headers = flow.get('request', {}).get('headers', {})
                    token = headers.get('token')
                    if token:
                        return token
        except Exception:
            pass
    return None


def sdgd_get_headers(token):
    app_id = os.environ.get('SDGD_APP_ID', '')
    referer = f"https://servicewechat.com/{app_id}/47/page-frame.html" if app_id else "https://servicewechat.com/"
    return {
        "token": token,
        "Content-Type": "application/json",
        "xweb_xhr": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "Referer": referer,
    }


def sdgd_api_request(token, endpoint, **params):
    url = f"{SDGD_BASE_URL}{endpoint}"
    try:
        response = requests.get(url, headers=sdgd_get_headers(token), params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data.get("errcode") != 0:
            return None
        return data.get("data", {})
    except requests.exceptions.RequestException:
        return None


def sdgd_get_all_channels(token):
    data = sdgd_api_request(token, "/live/channel/live/list/poster",
                            catalogId="quanbu", pageIndex=1, pageSize=200)
    return data.get("results", []) if data else []


def sdgd_get_epg_by_date(token, channel_id, date_str):
    data = sdgd_api_request(token, "/live/epg/list",
                            date=date_str, channelId=channel_id, posterType="HD")
    return data.get("results", []) if data else []


def sdgd_generate_dates_7plus1():
    dates = []
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    for i in range(7, 0, -1):
        dates.append((today - timedelta(days=i)).strftime('%Y-%m-%d'))
    dates.append(today.strftime('%Y-%m-%d'))
    dates.append((today + timedelta(days=1)).strftime('%Y-%m-%d'))
    return dates


def sdgd_fetch_channel_epg(args):
    token, channel, dates = args
    ch_id = channel.get('channelId')
    ch_name = channel.get('channelName', 'Unknown')
    result = {}
    for date_str in dates:
        programs = sdgd_get_epg_by_date(token, ch_id, date_str)
        result[date_str] = programs if programs is not None else []
    return ch_id, ch_name, result


def sdgd_fetch_all(token, channels, dates, max_workers=10):
    all_programs = {}
    task_args = [(token, ch, dates) for ch in channels]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(sdgd_fetch_channel_epg, args) for args in task_args]
        for future in as_completed(futures):
            try:
                ch_id, ch_name, result = future.result()
                all_programs[ch_id] = result
            except Exception as e:
                print(f"  [SDGD] 频道失败: {e}")
    return all_programs


def sdgd_clean_channel_name(name):
    if not name:
        return name
    if name.upper().startswith('CCTV-4K') or name.upper().startswith('CCTV4K'):
        return 'CCTV4K'
    cctv_num_pattern = r'^CCTV-?(\d+\+?)'
    match = re.match(cctv_num_pattern, name, re.IGNORECASE)
    if match:
        return f'CCTV{match.group(1)}'
    if name.upper().startswith('CCTV-'):
        return name[5:]
    for suffix in ['超高清', '高清', '标清']:
        if name.endswith(suffix):
            name = name[:-len(suffix)]
            break
    return name


def sdgd_fetch():
    """抓取 SDGD 数据，返回 (channels, programs)。无 token 返回 (None, None)"""
    print(f"\n=== SDGD 山东广电 API ===")
    token = sdgd_load_token()
    if not token:
        print("  [跳过] 未找到 SDGD_TOKEN / token_health.log / flows.json")
        return None, None
    print(f"  Token: {token[:12]}...")
    channels = sdgd_get_all_channels(token)
    if not channels:
        print("  [跳过] 无法获取频道列表，token 可能已过期")
        return None, None
    print(f"  频道数: {len(channels)}")
    dates = sdgd_generate_dates_7plus1()
    print(f"  日期: {dates[0]} ~ {dates[-1]} ({len(dates)} 天)")
    all_programs = sdgd_fetch_all(token, channels, dates)
    print(f"✓ SDGD 完成")
    return channels, all_programs


# ─────────────────────────── 4. FalseEPG 兜底 ───────────────────────────
class FalseEPGGenerator:
    """为无EPG的四川本地频道生成昨天、今天、明天节目单"""

    def __init__(self, channels):
        self.channels = channels

    @staticmethod
    def _format_datetime(dt):
        return dt.strftime('%Y%m%d%H%M%S') + ' +0800'

    def generate_programs(self):
        """返回 [{channel_id, start, stop, title}] 列表"""
        programs = []
        base_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        for day_offset in [-1, 0, 1]:
            day_date = base_date + timedelta(days=day_offset)
            for ch in self.channels:
                program_list = ch.get('programs', ["精彩节目", "电视剧场", "电影天地", "新闻播报"])
                for hour in range(24):
                    start_time = day_date + timedelta(hours=hour)
                    stop_time = start_time + timedelta(hours=1)
                    program_name = program_list[hour % len(program_list)]
                    programs.append({
                        'channel_id': ch['id'],
                        'title': program_name,
                        'start': start_time,
                        'stop': stop_time,
                    })
        return programs


# ─────────────────────────── 聚合输出 ───────────────────────────
def merge_and_build_xml(channel_aliases, programs):
    """
    channel_aliases: {ch_id: [alias, ...]}
    programs: [{channel_id, title, start(datetime 或 str), stop(...), desc(可选)}]
    返回合并后的 XMLTV Element
    """
    root = ET.Element('tv')

    for ch_id in sorted(channel_aliases):
        ch = ET.SubElement(root, 'channel')
        ch.set('id', ch_id)
        seen_names = set()
        for alias in channel_aliases[ch_id]:
            clean = clean_xml_text(alias)
            if clean and clean not in seen_names:
                dn = ET.SubElement(ch, 'display-name')
                dn.set('lang', 'zh')
                dn.text = clean
                seen_names.add(clean)

    def fmt(dt):
        if isinstance(dt, datetime):
            return dt.strftime('%Y%m%d%H%M%S') + ' +0800'
        return str(dt)

    for p in sorted(programs, key=lambda x: (x['channel_id'], fmt(x['start']))):
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
        if p.get('desc'):
            desc_el = ET.SubElement(prog, 'desc')
            desc_el.set('lang', 'zh')
            desc_el.text = clean_xml_text(p['desc'])

    return root


def main():
    parser = argparse.ArgumentParser(description='四川 EPG 聚合脚本 (TVMAO+TVSOU+SDGD+False)')
    parser.add_argument('--output', default='EPG', help='输出目录 (默认: EPG)')
    parser.add_argument('--filename', default='scepg', help='输出文件名前缀 (默认: scepg)')
    parser.add_argument('--no-tvmao', action='store_true', help='跳过 TVMAO')
    parser.add_argument('--no-tvsou', action='store_true', help='跳过 TVSOU')
    parser.add_argument('--no-sdgd', action='store_true', help='跳过 SDGD')
    parser.add_argument('--no-false', action='store_true', help='跳过 FalseEPG 兜底')
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    print("=" * 60)
    print("四川 EPG 聚合脚本启动")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    channel_aliases = {}   # ch_id -> list of aliases
    all_programs = []

    # ── 1. TVMAO ──
    if not args.no_tvmao:
        for pid in TVMAO_PROVINCES:
            crawler = TvmaoEPGCrawler(province_id=pid, output_dir=args.output)
            if crawler.crawl():
                crawler.remove_duplicates()
                for ch_id, (ch_name, aliases) in crawler.channels.items():
                    full = []
                    if ch_name:
                        full.append(ch_name)
                    full.extend(aliases)
                    if ch_id not in channel_aliases:
                        channel_aliases[ch_id] = full
                    else:
                        for a in full:
                            if a not in channel_aliases[ch_id]:
                                channel_aliases[ch_id].append(a)
                for p in crawler.programs:
                    all_programs.append(p)
            time.sleep(1.5)
    else:
        print("\n=== TVMAO 已跳过 ===")

    # ── 2. TVSOU ──
    if not args.no_tvsou and TVSOU_CHANNELS:
        tvsou = TvsouEPG(TVSOU_CHANNELS)
        for ch in TVSOU_CHANNELS:
            channel_aliases.setdefault(ch['id'], [ch['name']])
        for p in tvsou.fetch_all():
            def parse_tvsou_time(ts):
                return datetime.strptime(ts, '%Y%m%d%H%M%S +0800')
            all_programs.append({
                'channel_id': p['channel_id'],
                'title': p['title'],
                'start': parse_tvsou_time(p['start']),
                'stop': parse_tvsou_time(p['stop']),
            })
    elif not args.no_tvsou:
        print("\n=== TVSOU 跳过 (TVSOU_CHANNELS 为空) ===")

    # ── 3. SDGD ──
    if not args.no_sdgd:
        channels, sdgd_programs = sdgd_fetch()
        if channels:
            for ch in channels:
                cid = ch.get('channelId')
                name = sdgd_clean_channel_name(ch.get('channelName', ''))
                if cid:
                    channel_aliases.setdefault(cid, [name])
            beijing_tz = timezone(timedelta(hours=8))
            for cid, days in sdgd_programs.items():
                for date_str, progs in days.items():
                    for prog in progs:
                        start_ts = prog.get('startTimeStamps')
                        end_ts = prog.get('endTimeStamps')
                        title = prog.get('epgName', '')
                        if not title:
                            continue
                        if start_ts and end_ts:
                            start_dt = datetime.fromtimestamp(int(start_ts) / 1000, tz=beijing_tz).replace(tzinfo=None)
                            end_dt = datetime.fromtimestamp(int(end_ts) / 1000, tz=beijing_tz).replace(tzinfo=None)
                        else:
                            continue
                        all_programs.append({
                            'channel_id': cid,
                            'title': title,
                            'start': start_dt,
                            'stop': end_dt,
                        })
    else:
        print("\n=== SDGD 已跳过 ===")

    # ── 4. FalseEPG ──
    if not args.no_false and FALSE_EPG_CHANNELS:
        print(f"\n=== FalseEPG 兜底 ({len(FALSE_EPG_CHANNELS)} 频道) ===")
        fake = FalseEPGGenerator(FALSE_EPG_CHANNELS)
        for ch in FALSE_EPG_CHANNELS:
            channel_aliases.setdefault(ch['id'], [ch['name']])
        fake_programs = fake.generate_programs()
        all_programs.extend(fake_programs)
        print(f"✓ FalseEPG 完成: {len(fake_programs)} 节目")
    else:
        print("\n=== FalseEPG 已跳过 ===")

    # ── 去重 ──
    print("\n=== 去重 ===")
    seen = set()
    unique_programs = []
    for p in all_programs:
        key = (p['channel_id'], p['start'].strftime('%Y%m%d%H%M'), p['title'])
        if key not in seen:
            seen.add(key)
            unique_programs.append(p)
    all_programs = unique_programs
    print(f"去重后: {len(all_programs)} 节目, {len(channel_aliases)} 频道")

    # ── 生成 XML + GZ ──
    print("\n=== 生成 XMLTV ===")
    root = merge_and_build_xml(channel_aliases, all_programs)
    xml_path = os.path.join(args.output, args.filename + '.xml')
    gz_path = os.path.join(args.output, args.filename + '.xml.gz')
    write_xml_and_gz(root, xml_path, gz_path)

    print("\n" + "=" * 60)
    print("全部完成！")
    print(f"输出: {xml_path} / {gz_path}")
    print("=" * 60)


if __name__ == '__main__':
    main()
