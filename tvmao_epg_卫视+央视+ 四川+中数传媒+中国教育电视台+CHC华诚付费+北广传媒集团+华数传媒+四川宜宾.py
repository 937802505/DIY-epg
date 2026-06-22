#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电视猫EPG爬虫 - 优化版 (对齐sggc.xml标准格式，修复播放器XML解析错误)
修复要点：
  1. 去除DOCTYPE声明（sggc.xml模板无DOCTYPE，部分播放器遇到DOCTYPE会报错）
  2. channel id使用纯ASCII短ID（如CCTV1），别名写入display-name
  3. 时间格式严格对齐模板: YYYYMMDDHHmmss +0800（14位无空格）
  4. 保留clean_xml_text防止控制字符导致not well-formed
"""
import requests
from bs4 import BeautifulSoup
import re
import gzip
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import os
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 预编译正则：清理 XML 1.0 中非法的控制字符
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


class TvmaoEPGCrawler:
    """电视猫EPG爬虫"""
    def __init__(self, province_id='satellite', output_dir='EPG'):
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
        self.channels = {}   # raw_id -> ch_name
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
        """
        返回 (short_id, [alias1, alias2, ...])
        short_id: 纯ASCII短ID，用于 channel/@id 和 programme/@channel
        aliases:  所有别名（含中文）写入 display-name
        """
        match = re.search(r'/program/([^/]+)/([^/]+?)(?:-w\d+)?\.html', href or '')
        if match:
            code = match.group(2).upper().replace('CCTV-', 'CCTV').replace('TV1', '')
            mappings = {
                '1905电影网流金岁月': "1905电影网流金岁月,1905电影网",
                '1905电影网环球经典': "1905电影网环球经典",
                'AMC电影台': "AMC电影台,AMC",
                'ASTRO爱奇艺': "爱奇艺,ASTRO爱奇艺,Astro IQIYI",
                'Astro AEC': "Astro AEC",
                'Astro AOD': "Astro AOD",
                'Astro欢喜台': "Astro欢喜台",
                'BESTV': "BESTV游戏",
                'CCTV-央视文化精品': "CCTV-央视文化精品,CCTV文化精品,文化精品,CCTV央视文化精品,CCTV-央视文化,央视文化精品HD",
                'CCTV1': "CCTV1港澳版,CCTV1,CCTV-1,CCTV1综合,CCTV1-综合,CCTV-1综合,CCTV1高清,CCTV1咪咕,CCTV-1 综合[高清],CCTV1HD",
                'CCTV10': "CCTV10,CCTV-10,CCTV10科教,CCTV10-科教,CCTV-10科教,CCTV10高清,CCTV10咪咕,CCTV-10 科教[高清],CCTV10HD",
                'CCTV11': "CCTV11,CCTV-11,CCTV11-戏曲,CCTV11戏曲,CCTV11高清,CCTV11咪咕,CCTV-11 戏曲[高清],央视网CCTV11,CCTV11SD,CCTV11HD",
                'CCTV12': "CCTV12,CCTV-12,CCTV12社会与法,CCTV12-社会与法,CCTV-12社会与法,CCTV12高清,CCTV12咪咕,CCTV-12 社会与法[高清],央视网CCTV12,CCTV12HD",
                'CCTV13': "CCTV13,CCTV-13,CCTV13新闻,CCTV13-新闻,CCTV-13新闻,CCTV13高清,CCTV13咪咕,CCTV-13 新闻[高清],CCTV13HD",
                'CCTV14': "CCTV14,CCTV-14,CCTV14少儿,CCTV14-少儿,CCTV-14少儿,CCTV14高清,CCTV14咪咕,CCTV-14 少儿[高清],CCTV14HD",
                'CCTV15': "CCTV15,CCTV-15,CCTV15音乐,CCTV15-音乐,CCTV-15音乐,CCTV15高清,CCTV15咪咕,CCTV-15 音乐[高清],CCTV15HD,CCTV15SD",
                'CCTV16': "CCTV16,CCTV-164k,CCTV16奥林匹克,CCTV16-奥林匹克,CCTV-16奥林匹克,CCTV16奥林匹克4K,CCTV16高清,CCTV16咪咕,CCTV16-4k,CCTV-16 奥林匹克[高清],CCTV16HD,CCTV16 4K,CCTV164k",
                'CCTV17': "CCTV17,CCTV-17,CCTV17农业农村,CCTV17-农业农村,CCTV-17农业农村,CCTV17高清,CCTV17咪咕,CCTV-17 农业农村[高清],CCTV17SD,CCTV17HD",
                'CCTV2': "CCTV2,CCTV-2,CCTV2财经,CCTV2-财经,CCTV-2财经,CCTV2高清,CCTV2咪咕,CCTV-2 财经[高清],CCTV2HD",
                'CCTV3': "CCTV3,CCTV-3,CCTV3综艺,CCTV3-综艺,CCTV-3综艺,CCTV3高清,CCTV3咪咕,CCTV-3 综艺[高清],央视网CCTV3,CCTV3HD",
                'CCTV4': "CCTV4,CCTV-4,CCTV4中文国际,CCTV4-国际,CCTV-4中文国际,CCTV4高清,CCTV4咪咕,CCTV-4 中文国际[高清],CCTV4HD",
                'CCTV4K': "CCTV4K,CCTV4K超高清,CCTV-4K超高清,CCTV-4K 超高清[超清HDR]",
                'CCTV4欧洲': "CCTV4欧洲,CCTV4EUO,CCTV4-中文国际欧洲,CCTV-4中文国际欧洲,CCTV4-国际欧洲,CCTV-4欧洲,中文欧洲,央视欧洲,CCTV4(欧),cct4欧洲咪咕,CCTV-4 中文国际 欧洲[高清],CCTV4欧洲HD",
                'CCTV4美洲': "CCTV4美洲,CCTV4AME,CCTV4-中文国际美洲,CCTV-4中文国际美洲,CCTV4-国际美洲,CCTV-4美洲,中文美洲,央视美洲,CCTV4(美),cct4美洲咪咕,CCTV-4 中文国际 美洲[高清],CCTV4美洲HD",
                'CCTV5': "CCTV5,CCTV-5,CCTV5体育,CCTV5-体育,CCTV-5体育,CCTV5高清,CCTV5咪咕,CCTV-5 体育[高清],央视网CCTV5,CCTV5HD",
                'CCTV5+': "CCTV5+,CCTV-5+,CCTV5+体育赛事,CCTV-5+体育赛事,CCTV5+高清,CCTV-5+咪咕,CCTV-5+ 体育赛事[高清],CCTV5+HD",
                'CCTV6': "CCTV6,CCTV-6,CCTV6电影,CCTV6-电影,CCTV-6电影,CCTV6高清,CCTV6咪咕,CCTV-6 电影[高清],CCTV6HD",
                'CCTV7': "CCTV7,CCTV-7,CCTV7国防军事,CCTV7-军事,CCTV-7国防军事,CCTV7高清,CCTV7咪咕,CCTV-7 国防军事[高清],CCTV7HD",
                'CCTV8': "CCTV8,CCTV-8,CCTV8电视剧,CCTV8-电视剧,CCTV-8电视剧,CCTV8高清,CCTV8咪咕,CCTV-8 电视剧[高清],CCTV8HD",
                'CCTV8K': "CCTV8K,CCTV8K超高清,CCTV-8K超高清",
                'CCTV9': "CCTV9,CCTV-9,CCTV9纪录,CCTV9-纪录,CCTV-9记录,CCTV9高清,CCTV9咪咕,CCTV-9 纪录[高清],CCTV9HD",
                'CCTV世界地理': "CCTV世界地理,世界地理,CCTV-世界地理,世界地理HD",
                'CCTV中视购物': "CCTV中视购物,中视购物",
                'CCTV兵器科技': "CCTV兵器科技,兵器科技,CCTV-兵器科技,兵器科技HD",
                'CCTV卫生健康': "CCTV-卫生健康,CCTV卫生健康,卫生健康,卫生健康HD",
                'CCTV央视台球': "CCTV央视台球,央视台球,CCTV-央视台球,央视台球HD",
                'CCTV女性时尚': "CCTV女性时尚,女性时尚,CCTV-女性时尚,女性时尚HD",
                'CCTV娱乐': "CCTV娱乐,CCTV娱乐频道",
                'CCTV怀旧剧场': "CCTV怀旧剧场,怀旧剧场,CCTV-怀旧剧场,怀旧剧场HD",
                'CCTV戏曲': "CCTV戏曲,CCTV戏曲频道",
                'CCTV电视指南': "CCTV电视指南,电视指南,CCTV-电视指南,电视指南HD",
                'CCTV第一剧场': "CCTV第一剧场,第一剧场,CCTV-第一剧场,第一剧场HD",
                'CCTV风云剧场': "CCTV风云剧场,风云剧场,CCTV-风云剧场,风云剧场HD",
                'CCTV风云足球': "CCTV风云足球,风云足球,CCTV-风云足球,风云足球HD",
                'CCTV风云音乐': "CCTV风云音乐,风云音乐,CCTV-风云音乐,风云音乐HD",
                'CCTV高尔夫网球': "CCTV高尔夫网球,高尔夫网球,CCTV-高尔夫,高尔夫网球HD",
                'CDTV1': "CDTV1,CDTV1新闻综合,CDTV-1综合,成都新闻综合,成都综合,成都综合高清,成都新闻,CDTV1综合",
                'CDTV2': "CDTV2,CDTV-2经济,CDTV2经济资讯,成都经济,成都经济资讯,CDTV2经济",
                'CDTV3': "CDTV3,CDTV-3生活,CDTV3都市生活,成都生活,成都生活高清,成都都市,成都都市生活,CDTV3生活",
                'CDTV4': "CDTV4,CDTV-4影视,CDTV4影视文艺,成都影视,成都影视文艺,CDTV4影视",
                'CDTV5': "CDTV5,CDTV-5公共,CDTV5公共高清,成都公共,成都公共频道,成都公共高清,CDTV5公共",
                'CGTN英文': "CGTN英语,CGTN英文,CGTN,CGTN英语频道,CGTNHD",
                'CHC动作电影': "CHC动作电影,动作电影",
                'CHC家庭影院': "CHC家庭影院,家庭影院",
                'CHC影迷电影': "CHC影迷电影,影迷电影",
                '东方卫视': "东方卫视,东方卫视[超清HDR],东方卫视咪咕,东方卫视HD",
                '东南卫视': "东南卫视,东南卫视[高清],东南卫视HD",
                '中国教育1': "中国教育1,CETV1,CETV-1,CETV1[高清],CETV1HD",
                '中国教育2': "中国教育2,CETV2,CETV-2",
                '中国教育3': "中国教育3,CETV3,CETV-3",
                '中国教育4': "中国教育4,CETV4,CETV-4,CETV4HD",
                '中国电影频道': "中国电影频道,CMC,China Movie",
                '凤凰中文': "凤凰中文,凤凰中文频道,凤凰卫视中文,凤凰卫视",
                '凤凰资讯': "凤凰资讯,凤凰资讯台,凤凰资讯频道,凤凰卫视资讯",
                '凤凰电影': "凤凰电影,凤凰电影频道",
                '北京卫视': "北京卫视,北京卫视咪咕,北京卫视HD",
                '四川卫视': "四川卫视,四川卫视频道,四川卫视[超清HDR],四川卫视HD",
                '四川新闻': "四川新闻,SCTV新闻,SCTV-4新闻,四川新闻频道,四川新闻高清,SCTV4",
                '四川经济': "四川经济,SCTV经济,SCTV-2经济,四川经济频道,四川经济高清,SCTV2",
                '四川影视文艺': "四川影视文艺,SCTV影视文艺,SCTV-5影视,四川影视,四川影视高清,SCTV5",
                '四川文化旅游': "四川文化旅游,SCTV文旅,SCTV-3文旅,四川文旅,四川文旅高清,四川文化,SCTV3",
                '四川科教': "四川科教,SCTV-8,四川科教频道,四川科教高清,SCTV8",
                '四川妇女儿童': "四川妇女儿童,SCTV妇女儿童,SCTV-7妇儿,四川妇儿,四川妇儿高清,SCTV7",
                '四川乡村': "四川乡村,四川乡村频道,SCTV乡村,SCTV-9乡村,四川公共乡村,四川乡村高清",
                '四川星空购物': "四川星空购物,SCTV星空购物,SCTV-6购物,四川购物,四川购物高清,SCTV6",
                '四川峨眉电影': "四川峨眉电影,四川峨眉电影频道,峨眉电影",
                '康巴卫视': "康巴卫视,SCTV11康巴卫视,四川康巴卫视,康巴藏语综合",
                '天津卫视': "天津卫视,天津卫视[高清],天津卫视HD",
                '安徽卫视': "安徽卫视,安徽卫视[高清],安徽卫视HD",
                '山东卫视': "山东卫视,山东卫视咪咕,山东卫视[超清HDR],山东卫视HD",
                '上海五星体育': "上海五星体育,五星体育",
                '广东卫视': "广东卫视,广东卫视[超清HDR],广东卫视咪咕,广东卫视HD",
                '广东珠江': "广东珠江,珠江频道",
                '广西卫视': "广西卫视",
                '湖南卫视': "湖南卫视,湖南卫视[超清HDR],湖南卫视咪咕,湖南卫视HD",
                '湖北卫视': "湖北卫视,湖北卫视[高清],湖北卫视咪咕,湖北卫视HD",
                '江苏卫视': "江苏卫视,江苏卫视[超清HDR],江苏卫视咪咕,江苏卫视HD",
                '江西卫视': "江西卫视,江西卫视[高清],江西卫视咪咕,江西卫视HD",
                '浙江卫视': "浙江卫视,浙江卫视[超清HDR],浙江卫视咪咕,浙江卫视HD",
                '深圳卫视': "深圳卫视,深圳卫视咪咕,深圳卫视[超清HDR],深圳卫视HD",
                '海南卫视': "海南卫视,旅游卫视,海南卫视咪咕,旅游卫视HD",
                '海峡卫视': "海峡卫视,海峡频道,海峡卫视频道",
                '福建综合频道': "福建综合频道,福建综合",
                '辽宁卫视': "辽宁卫视,辽宁卫视[高清],辽宁卫视HD",
                '吉林卫视': "吉林卫视,吉林卫视[高清],吉林卫视HD",
                '黑龙江卫视': "黑龙江卫视,黑龙江视咪咕,黑龙江卫视[高清],黑龙江卫视HD",
                '云南卫视': "云南卫视,云南卫视HD",
                '贵州卫视': "贵州卫视,贵州卫视[高清],贵州卫视HD",
                '重庆卫视': "重庆卫视,重庆卫视[高清],重庆卫视HD",
                '西藏卫视': "西藏卫视,西藏卫视[高清],西藏卫视HD",
                '新疆卫视': "新疆卫视,新疆卫视[高清],新疆卫视HD",
                '宁夏卫视': "宁夏卫视,宁夏卫视[高清],宁夏卫视HD",
                '甘肃卫视': "甘肃卫视,甘肃卫视[高清],甘肃卫视HD",
                '青海卫视': "青海卫视,青海卫视HD",
                '内蒙古卫视': "内蒙古卫视",
                '山西卫视': "山西卫视",
                '河北卫视': "河北卫视,河北卫视[高清],河北卫视HD",
                '河南卫视': "河南卫视",
                '陕西卫视': "陕西卫视,陕西卫视咪咕",
                '三沙卫视': "三沙卫视",
                '兵团卫视': "兵团卫视,新疆兵团卫视,兵团卫视HD",
                '大湾区卫视': "南方卫视,大湾区卫视",
                'SITV东方财经': "SITV东方财经,东方财经,SiTV东方财经",
                'SITV劲爆体育': "SITV劲爆体育,SiTV劲爆体育,劲爆体育",
                'SITV都市剧场': "SITV都市剧场,都市剧场,SiTV都市频道",
                '第一财经': "第一财经,上海第一财经",
                '宜宾新闻': "宜宾综合,宜宾新闻,宜宾新闻频道,宜宾电视台新闻",
                '宜宾公共': "宜宾,宜宾公共,宜宾公共频道,宜宾电视台公共",
                '宜宾导视': "宜宾,宜宾导视,宜宾导视频道,宜宾电视台导视",
                '宜宾科教文旅': "宜宾科教文旅,宜宾科教,宜宾文旅,宜宾科教文旅频道",
            }
            alias_str = mappings.get(code)
            if alias_str:
                aliases = [a.strip() for a in alias_str.split(',') if a.strip()]
                return code, aliases
            # 未在映射表中：清理特殊字符作为id
            safe_id = re.sub(r'[^\w\-.]', '_', code)
            return safe_id, [code]

        # href解析失败时，用频道名称清理后作为id
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
                        # 存储 (ch_name, aliases) 供生成XML时使用
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

    def crawl_day(self, day_offset=0):
        if not self.today_w:
            return 0
        target_w, target_date = self.get_w_and_date(day_offset)
        added = 0
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
                added += future.result()
            time.sleep(0.2)
        return added

    def crawl(self):
        print(f"\n=== 开始抓取 {self.province_id} (仅今日) ===")
        if not self.detect_today_w():
            return False
        self.crawl_day(0)
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

    def generate_xmltv(self):
        """
        生成对齐 sggc.xml 格式的 XMLTV：
          - 无DOCTYPE声明
          - <?xml version="1.0" encoding="utf-8"?>
          - <tv> 根节点（无多余属性）
          - channel/display-name 列出所有别名
          - programme 时间格式：YYYYMMDDHHmmss +0800
          - programme 含 <title lang="zh"> 和 <category lang="zh">
        """
        self.remove_duplicates()
        root = ET.Element('tv')

        # 收集所有有节目的 channel_id
        channels_with_programs = set(p['channel_id'] for p in self.programs)

        for ch_id in sorted(channels_with_programs):
            ch_info = self.channels.get(ch_id)
            if ch_info:
                ch_name, aliases = ch_info
            else:
                ch_name, aliases = ch_id, [ch_id]

            ch = ET.SubElement(root, 'channel')
            ch.set('id', ch_id)

            seen_names = set()
            # 先添加网页抓到的频道名
            if ch_name:
                clean = clean_xml_text(ch_name)
                if clean and clean not in seen_names:
                    dn = ET.SubElement(ch, 'display-name')
                    dn.set('lang', 'zh')
                    dn.text = clean
                    seen_names.add(clean)
            # 再添加所有别名（提升播放器匹配率）
            for alias in aliases:
                clean = clean_xml_text(alias)
                if clean and clean not in seen_names:
                    dn = ET.SubElement(ch, 'display-name')
                    dn.set('lang', 'zh')
                    dn.text = clean
                    seen_names.add(clean)

        # 节目单：按频道+开始时间排序
        # 时间格式严格对齐 sggc.xml: YYYYMMDDHHmmss +0800
        FMT = '%Y%m%d%H%M%S +0800'
        for p in sorted(self.programs, key=lambda x: (x['channel_id'], x['start'])):
            title_text = clean_xml_text(p['title'])
            if not title_text:
                continue
            prog = ET.SubElement(root, 'programme')
            prog.set('start', p['start'].strftime(FMT))
            prog.set('stop', p['stop'].strftime(FMT))
            prog.set('channel', p['channel_id'])
            title_el = ET.SubElement(prog, 'title')
            title_el.set('lang', 'zh')
            title_el.text = title_text
            cat_el = ET.SubElement(prog, 'category')
            cat_el.set('lang', 'zh')
            cat_el.text = '电视剧'

        return root

    def save(self, filename='epg.xml'):
        if not self.channels:
            return None
        os.makedirs(self.output_dir, exist_ok=True)
        root = self.generate_xmltv()
        indent_xml(root)

        xml_path = os.path.join(self.output_dir, filename)

        # ─────────────────────────────────────────────────────────────
        # 关键修复：手动写出 XML，不使用 ElementTree.write() 的 xml_declaration
        # 以便精确控制头部，确保与 sggc.xml 一致：
        #   <?xml version="1.0" encoding="utf-8"?>
        #   <tv>
        #     ...
        #   </tv>
        # 无 DOCTYPE，无 generator-info-name 属性
        # ─────────────────────────────────────────────────────────────
        tree = ET.ElementTree(root)
        # 先写到临时字符串
        import io
        buf = io.BytesIO()
        tree.write(buf, encoding='utf-8', xml_declaration=False, method='xml')
        xml_body = buf.getvalue().decode('utf-8')

        # 组合最终内容：标准xml声明 + 换行 + 正文
        content = '<?xml version="1.0" encoding="utf-8"?>\n' + xml_body + '\n'

        with open(xml_path, 'w', encoding='utf-8') as f:
            f.write(content)

        # 生成 gzip 压缩版
        gz_path = xml_path + '.gz'
        with gzip.open(gz_path, 'wt', encoding='utf-8') as f:
            f.write(content)

        print(f"  保存完成: {xml_path} ({os.path.getsize(xml_path):,} bytes)")
        return xml_path


def main():
    parser = argparse.ArgumentParser(description='电视猫全覆盖EPG爬虫 (仅今日)')
    parser.add_argument('--output', default='EPG', help='输出目录')
    args = parser.parse_args()

    province_list = [
        'TJTV', 'JSTV', 'JXTV', 'GDTV', 'GUIZOUTV', 'NXTV', 'HEBEI', 'LNTV', 'ZJTV',
        'SDTV', 'HNTV', 'GUANXI', 'YNTV', 'SHXITV', 'XJTV', 'SXTV', 'JILIN', 'AHTV',
        'HUBEI', 'TCTC', 'CCQTV', 'XIZANGTV', 'GSTV', 'BTV', 'NMGTV', 'HLJTV', 'FJTV',
        'HUNANTV', 'QHTV', 'SCTV', 'CETV', 'CCTV', 'CCTVPAYFEE', 'CHC', 'HSCM',
        'CHENGDU', 'SCYBTV', 'SCMYTV', 'SHHAI', 'satellite', 'BAMC',
    ]
    # 去重保序
    seen = set()
    province_list = [p for p in province_list if not (p in seen or seen.add(p))]

    print("=== 电视猫全覆盖EPG爬虫启动 (仅抓取今日) ===")
    print(f"计划抓取地区: {len(province_list)} 个")

    all_channels = {}
    all_programs = []
    success = 0

    for pid in province_list:
        crawler = TvmaoEPGCrawler(province_id=pid, output_dir=args.output)
        if crawler.crawl():
            all_channels.update(crawler.channels)
            all_programs.extend(crawler.programs)
            success += 1
        time.sleep(1.5)

    final = TvmaoEPGCrawler('merged', args.output)
    final.channels = all_channels
    final.programs = all_programs
    final.save('sggc-epg.xml')

    print(f"\n=== 全部完成！===")
    print(f"成功: {success}/{len(province_list)} | 唯一频道: {len(all_channels)} | 节目: {len(all_programs)}")


if __name__ == '__main__':
    main()
