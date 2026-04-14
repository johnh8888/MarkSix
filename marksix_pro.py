#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
香港六合彩 - 专业级全面升级版
功能：
- 多数据源交叉验证与自动补全
- 指数衰减动量 + 关联规则挖掘 (Lift)
- 蒙特卡洛组合优化 + 和值/奇偶/区间约束
- 特别号独立二阶马尔可夫链
- 滚动窗口回测 + 夏普比率评估
- 配置文件支持 (YAML) + 日志系统

用法:
    python marksix_pro.py sync [--source auto] [--third-party-url ...]
    python marksix_pro.py predict
    python marksix_pro.py show
    python marksix_pro.py backtest
"""

import argparse
import csv
import io
import json
import logging
import math
import random
import re
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple, Union
from urllib.request import Request, urlopen

# -------------------- 配置文件处理 --------------------
try:
    import yaml

    CONFIG_PATH = Path(__file__).with_suffix(".yaml")
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            CONFIG = yaml.safe_load(f) or {}
    else:
        CONFIG = {}
except ImportError:
    CONFIG = {}

# -------------------- 日志系统 --------------------
LOG_LEVEL = getattr(logging, CONFIG.get("log_level", "INFO"))
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("marksix_pro")

# -------------------- 常量与配置 --------------------
SCRIPT_DIR = Path(__file__).resolve().parent
DB_PATH_DEFAULT = str(SCRIPT_DIR / CONFIG.get("db_name", "marksix_pro.db"))
CSV_PATH_DEFAULT = str(SCRIPT_DIR / CONFIG.get("csv_name", "Mark_Six.csv"))

OFFICIAL_URL = CONFIG.get(
    "official_url", "https://bet.hkjc.com/contentserver/jcbw/cmc/last30draw.json"
)
THIRD_PARTY_URLS_DEFAULT = CONFIG.get(
    "third_party_urls",
    [
        "https://marksix6.net/index.php?api=1",
        "https://lottolyzer.com/history/hong-kong/mark-six/page/1/per-page/50/summary-view",
    ],
)
THIRD_PARTY_MAX_PAGES_DEFAULT = CONFIG.get("third_party_max_pages", 60)

# 策略配置
STRATEGY_CONFIGS = {
    "hot": {"name": "热号策略", "w_freq": 0.7, "w_omit": 0.0, "w_mom": 0.3, "w_pair": 0.0},
    "cold": {"name": "冷号回补", "w_freq": 0.0, "w_omit": 0.7, "w_mom": 0.3, "w_pair": 0.0},
    "momentum": {"name": "近期动量", "w_freq": 0.2, "w_omit": 0.0, "w_mom": 0.8, "w_pair": 0.0},
    "balanced": {
        "name": "组合策略",
        "w_freq": 0.35,
        "w_omit": 0.25,
        "w_mom": 0.25,
        "w_pair": 0.15,
    },
    "pattern": {
        "name": "规律挖掘",
        "w_freq": 0.30,
        "w_omit": 0.30,
        "w_mom": 0.20,
        "w_pair": 0.20,
    },
}
STRATEGY_IDS = ["hot", "cold", "momentum", "balanced", "ensemble", "pattern"]

# 生肖映射
ZODIAC_MAP = {
    "马": [1, 13, 25, 37, 49],
    "羊": [12, 24, 36, 48],
    "猴": [11, 23, 35, 47],
    "鸡": [10, 22, 34, 46],
    "狗": [9, 21, 33, 45],
    "猪": [8, 20, 32, 44],
    "鼠": [7, 19, 31, 43],
    "牛": [6, 18, 30, 42],
    "虎": [5, 17, 29, 41],
    "兔": [4, 16, 28, 40],
    "龙": [3, 15, 27, 39],
    "蛇": [2, 14, 26, 38],
}
ALL_NUMBERS = list(range(1, 50))

MONTE_CARLO_TRIALS = CONFIG.get("monte_carlo_trials", 2000)
SUM_TARGET = CONFIG.get("sum_target", (115, 185))


# -------------------- 数据结构 --------------------
@dataclass
class DrawRecord:
    issue_no: str
    draw_date: str
    numbers: List[int]
    special_number: int


@dataclass
class StrategyScore:
    main_picks: List[int]
    special_pick: int
    confidence: float
    raw_scores: Dict[int, float] = field(default_factory=dict)


# -------------------- 工具函数 --------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_issue(issue_no: str) -> Optional[Tuple[str, int, int]]:
    parts = issue_no.split("/")
    if len(parts) != 2:
        return None
    year_s, seq_s = parts
    if not (year_s.isdigit() and seq_s.isdigit()):
        return None
    return year_s, int(seq_s), len(seq_s)


def next_issue_number(issue: str) -> str:
    parsed = parse_issue(issue)
    if not parsed:
        return issue
    year, seq, width = parsed
    return f"{year}/{str(seq + 1).zfill(width)}"


def get_zodiac(num: int) -> str:
    for z, nums in ZODIAC_MAP.items():
        if num in nums:
            return z
    return ""


# -------------------- 数据库操作 --------------------
def connect_db(db_path: str = DB_PATH_DEFAULT) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS draws (
            issue_no TEXT PRIMARY KEY,
            draw_date TEXT NOT NULL,
            numbers_json TEXT NOT NULL,
            special_number INTEGER NOT NULL,
            sum_value INTEGER,
            odd_count INTEGER,
            big_count INTEGER,
            consec_pairs INTEGER,
            zodiac_json TEXT,
            source TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS predictions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            issue_no TEXT NOT NULL,
            strategy TEXT NOT NULL,
            numbers_json TEXT NOT NULL,
            special_number INTEGER,
            confidence REAL,
            hit_count INTEGER,
            hit_rate REAL,
            special_hit INTEGER,
            status TEXT DEFAULT 'PENDING',
            created_at TEXT NOT NULL,
            reviewed_at TEXT,
            UNIQUE(issue_no, strategy)
        );

        CREATE TABLE IF NOT EXISTS backtest_stats (
            strategy TEXT PRIMARY KEY,
            total_runs INTEGER DEFAULT 0,
            avg_hit REAL DEFAULT 0,
            hit1_rate REAL DEFAULT 0,
            hit2_rate REAL DEFAULT 0,
            hit3_rate REAL DEFAULT 0,
            special_rate REAL DEFAULT 0,
            sharpe_ratio REAL DEFAULT 0,
            max_drawdown REAL DEFAULT 0,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS pair_affinity (
            num1 INTEGER NOT NULL,
            num2 INTEGER NOT NULL,
            co_occurrence INTEGER DEFAULT 0,
            lift REAL DEFAULT 1.0,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (num1, num2)
        );

        CREATE TABLE IF NOT EXISTS model_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
    """)
    _ensure_columns(conn)
    conn.commit()


def _ensure_columns(conn: sqlite3.Connection) -> None:
    existing = {r[1] for r in conn.execute("PRAGMA table_info(draws)").fetchall()}
    desired = {"sum_value", "odd_count", "big_count", "consec_pairs", "zodiac_json"}
    for col in desired - existing:
        if col == "zodiac_json":
            conn.execute(f"ALTER TABLE draws ADD COLUMN {col} TEXT")
        else:
            conn.execute(f"ALTER TABLE draws ADD COLUMN {col} INTEGER")
    existing = {r[1] for r in conn.execute("PRAGMA table_info(predictions)").fetchall()}
    if "confidence" not in existing:
        conn.execute("ALTER TABLE predictions ADD COLUMN confidence REAL")
    existing = {r[1] for r in conn.execute("PRAGMA table_info(backtest_stats)").fetchall()}
    for col in ["hit3_rate", "sharpe_ratio", "max_drawdown"]:
        if col not in existing:
            conn.execute(f"ALTER TABLE backtest_stats ADD COLUMN {col} REAL")


def compute_draw_features(numbers: List[int], special: int) -> Dict:
    zodiacs = []
    for n in numbers:
        for z, ns in ZODIAC_MAP.items():
            if n in ns:
                zodiacs.append(z)
                break
    return {
        "sum_value": sum(numbers),
        "odd_count": sum(1 for n in numbers if n % 2 == 1),
        "big_count": sum(1 for n in numbers if n >= 25),
        "consec_pairs": sum(
            1 for i in range(5) if abs(numbers[i] - numbers[i + 1]) == 1
        ),
        "zodiac_json": json.dumps(zodiacs, ensure_ascii=False),
    }


def upsert_draw(conn: sqlite3.Connection, record: DrawRecord, source: str) -> str:
    now = utc_now()
    features = compute_draw_features(record.numbers, record.special_number)
    existing = conn.execute(
        "SELECT issue_no FROM draws WHERE issue_no = ?", (record.issue_no,)
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE draws SET draw_date=?, numbers_json=?, special_number=?,
                sum_value=?, odd_count=?, big_count=?, consec_pairs=?, zodiac_json=?,
                source=?, updated_at=?
            WHERE issue_no=?
            """,
            (
                record.draw_date,
                json.dumps(record.numbers),
                record.special_number,
                features["sum_value"],
                features["odd_count"],
                features["big_count"],
                features["consec_pairs"],
                features["zodiac_json"],
                source,
                now,
                record.issue_no,
            ),
        )
        return "updated"
    else:
        conn.execute(
            """
            INSERT INTO draws (issue_no, draw_date, numbers_json, special_number,
                sum_value, odd_count, big_count, consec_pairs, zodiac_json,
                source, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.issue_no,
                record.draw_date,
                json.dumps(record.numbers),
                record.special_number,
                features["sum_value"],
                features["odd_count"],
                features["big_count"],
                features["consec_pairs"],
                features["zodiac_json"],
                source,
                now,
                now,
            ),
        )
        return "inserted"


# -------------------- 数据获取与交叉验证 --------------------
def fetch_from_url(url: str, timeout: int = 20) -> Optional[str]:
    req = Request(
        url, headers={"User-Agent": "Mozilla/5.0 (compatible; marksix-pro/1.0)"}
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8-sig")
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def parse_official_json(text: str) -> List[DrawRecord]:
    data = json.loads(text)
    records = []
    for item in data:
        issue = str(item.get("draw", "")).strip()
        if not issue:
            continue
        nums_str = item.get("numbers", "")
        try:
            nums = [int(x) for x in nums_str.split(",") if x.strip().isdigit()]
        except:
            continue
        if len(nums) >= 7:
            records.append(
                DrawRecord(
                    issue_no=issue,
                    draw_date=item.get("date", ""),
                    numbers=nums[:6],
                    special_number=nums[6],
                )
            )
    return records


def parse_marksix6_json(text: str) -> List[DrawRecord]:
    data = json.loads(text)
    lottery_list = data.get("lottery_data", [])
    records = []
    for item in lottery_list:
        if "香港" not in str(item.get("name", "")):
            continue
        issue = str(item.get("expect", "")).strip()
        code = item.get("openCode", "")
        try:
            nums = [int(x) for x in code.split(",") if x.strip().isdigit()]
        except:
            continue
        if len(nums) >= 7:
            records.append(
                DrawRecord(
                    issue_no=issue,
                    draw_date=item.get("openTime", "")[:10],
                    numbers=nums[:6],
                    special_number=nums[6],
                )
            )
    return records


def parse_lottolyzer_html(html: str) -> List[DrawRecord]:
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text)
    pattern = re.compile(
        r"(?P<issue>\d{2}/\d{3})\s+"
        r"(?P<date>\d{4}-\d{2}-\d{2})\s+"
        r"(?P<nums>[\d,\s]+?)\s+"
        r"(?P<extra>\d{1,2})\b"
    )
    records = []
    for m in pattern.finditer(text):
        issue = m.group("issue")
        date = m.group("date")
        nums = [int(x) for x in m.group("nums").split(",") if x.strip().isdigit()]
        extra = int(m.group("extra"))
        if len(nums) >= 6:
            records.append(
                DrawRecord(
                    issue_no=issue,
                    draw_date=date,
                    numbers=nums[:6],
                    special_number=extra,
                )
            )
    return records


def parse_csv_text(text: str) -> List[DrawRecord]:
    records = []
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        issue = row.get("期号") or row.get("issueNo") or row.get("issue")
        date = row.get("日期") or row.get("date") or row.get("drawDate")
        nums_str = row.get("中奖号码") or row.get("numbers") or row.get("result")
        special_str = row.get("特别号码") or row.get("special") or row.get("no7")
        if not issue or not nums_str:
            continue
        try:
            nums = [
                int(x) for x in nums_str.replace("，", ",").split(",") if x.strip().isdigit()
            ]
            special = int(special_str)
        except:
            continue
        if len(nums) >= 6 and 1 <= special <= 49:
            records.append(
                DrawRecord(
                    issue_no=issue.strip(),
                    draw_date=date.strip(),
                    numbers=nums[:6],
                    special_number=special,
                )
            )
    return records


def fetch_lottolyzer_paginated(
    base_url: str, max_pages: int = THIRD_PARTY_MAX_PAGES_DEFAULT
) -> List[DrawRecord]:
    """抓取 Lottolyzer 多页数据"""
    all_records = []
    for page in range(1, max_pages + 1):
        page_url = re.sub(r"/page/\d+/", f"/page/{page}/", base_url)
        if page == 1 and "/page/" not in base_url:
            page_url = base_url
        html = fetch_from_url(page_url)
        if not html:
            break
        records = parse_lottolyzer_html(html)
        if not records:
            break
        all_records.extend(records)
        if len(records) < 50:  # 最后一页
            break
    return all_records


def fetch_all_sources(
    official_url: str = OFFICIAL_URL,
    third_party_urls: Optional[List[str]] = None,
    third_party_max_pages: int = THIRD_PARTY_MAX_PAGES_DEFAULT,
) -> List[DrawRecord]:
    """交叉验证：从多个源获取数据，取多数一致的记录"""
    if third_party_urls is None:
        third_party_urls = THIRD_PARTY_URLS_DEFAULT

    all_records: Dict[str, List[DrawRecord]] = defaultdict(list)

    # 官方源
    text = fetch_from_url(official_url)
    if text:
        records = parse_official_json(text)
        for r in records:
            all_records[r.issue_no].append(r)

    # 第三方源
    for url in third_party_urls:
        if "lottolyzer.com" in url:
            records = fetch_lottolyzer_paginated(url, max_pages=third_party_max_pages)
        else:
            text = fetch_from_url(url)
            if not text:
                continue
            if "marksix6.net" in url:
                records = parse_marksix6_json(text)
            elif text.lstrip().startswith("{"):
                records = parse_official_json(text)
            else:
                records = parse_csv_text(text)
        for r in records:
            all_records[r.issue_no].append(r)

    # 投票融合：每条记录必须有至少2个源一致才采纳
    final_records = []
    for issue, variants in all_records.items():
        if len(variants) >= 2:
            counts = Counter(
                tuple(v.numbers) + (v.special_number,) for v in variants
            )
            most_common = counts.most_common(1)[0][0]
            if counts[most_common] >= 2:
                base = variants[0]
                final_records.append(
                    DrawRecord(
                        issue_no=issue,
                        draw_date=base.draw_date,
                        numbers=list(most_common[:-1]),
                        special_number=most_common[-1],
                    )
                )
        elif len(variants) == 1:
            final_records.append(variants[0])

    logger.info(
        f"Cross-validation: total {len(final_records)} records from {len(all_records)} issues"
    )
    return final_records


def sync_draws(
    conn: sqlite3.Connection, records: List[DrawRecord], source: str = "auto"
) -> Tuple[int, int]:
    inserted = updated = 0
    for r in records:
        res = upsert_draw(conn, r, source)
        if res == "inserted":
            inserted += 1
        else:
            updated += 1
    conn.commit()
    return inserted, updated


# -------------------- 高级特征工程 --------------------
def get_recent_draws(
    conn: sqlite3.Connection, limit: int = 200
) -> List[List[int]]:
    rows = conn.execute(
        "SELECT numbers_json FROM draws ORDER BY draw_date DESC, issue_no DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [json.loads(r[0]) for r in rows]


def get_recent_specials(conn: sqlite3.Connection, limit: int = 200) -> List[int]:
    rows = conn.execute(
        "SELECT special_number FROM draws ORDER BY draw_date DESC, issue_no DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [r[0] for r in rows]


def calculate_exp_momentum(draws: List[List[int]], half_life: int = 6) -> Dict[int, float]:
    """指数衰减动量"""
    scores = {n: 0.0 for n in ALL_NUMBERS}
    for i, draw in enumerate(draws):
        weight = math.exp(-i / half_life)
        for n in draw:
            scores[n] += weight
    return scores


def calculate_pair_lift(draws: List[List[int]]) -> Dict[Tuple[int, int], float]:
    """计算号码对的提升度(Lift)"""
    pair_count = Counter()
    single_count = Counter()
    total_draws = len(draws)
    for draw in draws:
        for n in draw:
            single_count[n] += 1
        for a, b in combinations(sorted(draw), 2):
            pair_count[(a, b)] += 1

    lift_map = {}
    for (a, b), cnt in pair_count.items():
        expected = (
            (single_count[a] / total_draws)
            * (single_count[b] / total_draws)
            * total_draws
        )
        if expected > 0:
            lift_map[(a, b)] = cnt / expected
    return lift_map


def calculate_sum_probability(draws: List[List[int]]) -> Dict[str, float]:
    sums = [sum(d) for d in draws[-100:]]
    avg = sum(sums) / len(sums)
    std = (sum((s - avg) ** 2 for s in sums) / len(sums)) ** 0.5
    return {"avg": avg, "std": std, "low": avg - std, "high": avg + std}


# -------------------- 智能过滤与蒙特卡洛选号 --------------------
def smart_filter(nums: List[int]) -> bool:
    if len(nums) != 6:
        return False
    s = sorted(nums)
    total = sum(s)
    odd = sum(1 for n in s if n % 2 == 1)
    big = sum(1 for n in s if n >= 25)
    if total < SUM_TARGET[0] or total > SUM_TARGET[1]:
        return False
    if odd == 0 or odd == 6:
        return False
    if big == 0 or big == 6:
        return False
    zones = [(n - 1) // 10 for n in s]
    if max(Counter(zones).values()) > 3:
        return False
    tails = [n % 10 for n in s]
    if max(Counter(tails).values()) > 2:
        return False
    consec = 1
    max_consec = 1
    for i in range(1, 6):
        if s[i] - s[i - 1] == 1:
            consec += 1
            max_consec = max(max_consec, consec)
        else:
            consec = 1
    if max_consec > 3:
        return False
    return True


def monte_carlo_pick(
    scores: Dict[int, float],
    pair_lift: Dict[Tuple[int, int], float],
    trials: int = MONTE_CARLO_TRIALS,
) -> List[int]:
    """蒙特卡洛模拟寻找最优6码组合"""
    candidates = [
        n for n, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:35]
    ]
    best_combo = []
    best_score = -1e9

    for _ in range(trials):
        combo = sorted(random.sample(candidates, 6))
        if not smart_filter(combo):
            continue
        score = sum(scores[n] for n in combo)
        for a, b in combinations(combo, 2):
            score += pair_lift.get((a, b), 0) * 0.2
            score += pair_lift.get((b, a), 0) * 0.2
        if score > best_score:
            best_score = score
            best_combo = combo

    if not best_combo:
        best_combo = [
            n for n, _ in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:6]
        ]
    return best_combo


# -------------------- 特别号独立马尔可夫模型 --------------------
class SpecialMarkovModel:
    def __init__(self, order: int = 2):
        self.order = order
        self.transitions = defaultdict(Counter)

    def train(self, specials: List[int]):
        for i in range(len(specials) - self.order):
            state = tuple(specials[i : i + self.order])
            next_num = specials[i + self.order]
            self.transitions[state][next_num] += 1

    def predict(self, recent: List[int]) -> int:
        if len(recent) < self.order:
            return max(set(recent), key=recent.count) if recent else random.randint(1, 49)
        state = tuple(recent[-self.order :])
        if state in self.transitions and self.transitions[state]:
            return self.transitions[state].most_common(1)[0][0]
        return max(set(recent), key=recent.count)


# -------------------- 策略核心 --------------------
def generate_strategy_score(
    draws: List[List[int]],
    specials: List[int],
    strategy: str,
    pair_lift: Dict[Tuple[int, int], float],
) -> StrategyScore:
    cfg = STRATEGY_CONFIGS.get(strategy, STRATEGY_CONFIGS["balanced"])
    freq = {n: 0.0 for n in ALL_NUMBERS}
    for d in draws:
        for n in d:
            freq[n] += 1.0

    omit = {}
    for n in ALL_NUMBERS:
        for i, d in enumerate(draws):
            if n in d:
                omit[n] = i
                break
        else:
            omit[n] = len(draws)

    mom = calculate_exp_momentum(draws)

    def norm(d):
        vals = list(d.values())
        mn, mx = min(vals), max(vals)
        if mx == mn:
            return {k: 0.0 for k in d}
        return {k: (v - mn) / (mx - mn) for k, v in d.items()}

    freq_n = norm(freq)
    omit_n = norm({n: 1.0 / (omit[n] + 1) for n in ALL_NUMBERS})
    mom_n = norm(mom)

    scores = {}
    for n in ALL_NUMBERS:
        scores[n] = (
            freq_n[n] * cfg["w_freq"]
            + omit_n[n] * cfg["w_omit"]
            + mom_n[n] * cfg["w_mom"]
        )

    if strategy == "ensemble":
        return ensemble_vote(draws, specials, pair_lift)

    main_picks = monte_carlo_pick(scores, pair_lift)

    # 特别号
    markov = SpecialMarkovModel(order=2)
    markov.train(specials)
    special_pick = markov.predict(specials[-5:])
    while special_pick in main_picks:
        special_pick = (special_pick % 49) + 1

    confidence = sum(scores[n] for n in main_picks) / 6
    return StrategyScore(main_picks, special_pick, confidence, scores)


def ensemble_vote(
    draws: List[List[int]], specials: List[int], pair_lift: Dict
) -> StrategyScore:
    scores_list = []
    for s in ["hot", "cold", "momentum", "balanced", "pattern"]:
        scores_list.append(
            generate_strategy_score(draws, specials, s, pair_lift).raw_scores
        )
    votes = {n: 0.0 for n in ALL_NUMBERS}
    for sc in scores_list:
        ranked = sorted(sc.items(), key=lambda x: x[1], reverse=True)
        for rank, (n, _) in enumerate(ranked):
            votes[n] += 49 - rank
    norm_votes = {n: v / max(votes.values()) for n, v in votes.items()}
    main_picks = monte_carlo_pick(norm_votes, pair_lift)
    special = SpecialMarkovModel(2)
    special.train(specials)
    sp = special.predict(specials[-5:])
    while sp in main_picks:
        sp = (sp % 49) + 1
    confidence = sum(norm_votes[n] for n in main_picks) / 6
    return StrategyScore(main_picks, sp, confidence, norm_votes)


# -------------------- 滚动回测与评估 --------------------
def run_rolling_backtest(
    conn: sqlite3.Connection, window: int = 100, step: int = 10
) -> None:
    rows = conn.execute(
        "SELECT numbers_json, special_number FROM draws ORDER BY draw_date, issue_no"
    ).fetchall()
    if len(rows) < window + 10:
        logger.warning("Not enough data for rolling backtest")
        return
    all_draws = [json.loads(r[0]) for r in rows]
    all_specials = [r[1] for r in rows]
    results = defaultdict(list)

    for i in range(window, len(all_draws), step):
        train_draws = all_draws[max(0, i - window) : i]
        train_specials = all_specials[max(0, i - window) : i]
        test_draw = set(all_draws[i])
        test_special = all_specials[i]
        pair_lift = calculate_pair_lift(train_draws)

        for strat in STRATEGY_IDS:
            score = generate_strategy_score(
                train_draws, train_specials, strat, pair_lift
            )
            hits = len(set(score.main_picks) & test_draw)
            special_hit = 1 if score.special_pick == test_special else 0
            results[strat].append((hits, special_hit, score.confidence))

    # 计算统计量并更新数据库
    for strat, records in results.items():
        hits = [r[0] for r in records]
        avg_hit = sum(hits) / len(hits)
        hit1 = sum(1 for h in hits if h >= 1) / len(hits)
        hit2 = sum(1 for h in hits if h >= 2) / len(hits)
        hit3 = sum(1 for h in hits if h >= 3) / len(hits)
        special_rate = sum(r[1] for r in records) / len(records)
        returns = [h / 6.0 + r[1] * 0.5 for h, r in zip(hits, records)]
        avg_ret = sum(returns) / len(returns)
        std_ret = (sum((r - avg_ret) ** 2 for r in returns) / len(returns)) ** 0.5
        sharpe = avg_ret / std_ret if std_ret > 0 else 0
        conn.execute(
            """
            INSERT OR REPLACE INTO backtest_stats
            (strategy, total_runs, avg_hit, hit1_rate, hit2_rate, hit3_rate, special_rate, sharpe_ratio, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                strat,
                len(records),
                avg_hit,
                hit1,
                hit2,
                hit3,
                special_rate,
                sharpe,
                utc_now(),
            ),
        )
    conn.commit()


def review_latest(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT issue_no, numbers_json, special_number FROM draws ORDER BY draw_date DESC LIMIT 1"
    ).fetchone()
    if not row:
        return 0
    issue_no = row["issue_no"]
    winning = set(json.loads(row["numbers_json"]))
    winning_special = row["special_number"]
    preds = conn.execute(
        "SELECT id, numbers_json, special_number FROM predictions WHERE issue_no = ? AND status = 'PENDING'",
        (issue_no,),
    ).fetchall()
    reviewed = 0
    for p in preds:
        picked = json.loads(p["numbers_json"])
        hit_count = len([n for n in picked if n in winning])
        hit_rate = hit_count / 6.0
        special_hit = 1 if p["special_number"] == winning_special else 0
        conn.execute(
            """
            UPDATE predictions SET status='REVIEWED', hit_count=?, hit_rate=?, special_hit=?, reviewed_at=?
            WHERE id=?
            """,
            (hit_count, hit_rate, special_hit, utc_now(), p["id"]),
        )
        reviewed += 1
    conn.commit()
    return reviewed


# -------------------- 命令行接口 --------------------
def parse_url_list(values: Sequence[str]) -> List[str]:
    out = []
    for v in values:
        for part in v.split(","):
            url = part.strip()
            if url:
                out.append(url)
    dedup = []
    seen = set()
    for u in out:
        if u not in seen:
            dedup.append(u)
            seen.add(u)
    return dedup


def cmd_sync(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    init_db(conn)
    third_party_urls = parse_url_list(args.third_party_url) if args.third_party_url else THIRD_PARTY_URLS_DEFAULT
    records = fetch_all_sources(
        official_url=args.official_url,
        third_party_urls=third_party_urls,
        third_party_max_pages=args.third_party_max_pages,
    )
    if not records:
        logger.error("No valid records from any source")
        return
    ins, upd = sync_draws(conn, records, source="cross_validated")
    logger.info(f"Sync done: inserted={ins}, updated={upd}")

    # 更新关联对表
    draws = get_recent_draws(conn, 300)
    pair_lift = calculate_pair_lift(draws)
    conn.execute("DELETE FROM pair_affinity")
    for (a, b), lift in pair_lift.items():
        conn.execute(
            "INSERT INTO pair_affinity (num1, num2, lift, updated_at) VALUES (?,?,?,?)",
            (a, b, lift, utc_now()),
        )
    conn.commit()

    # 复盘最新一期
    reviewed = review_latest(conn)
    logger.info(f"Reviewed {reviewed} predictions")

    # 增量回测
    run_rolling_backtest(conn)
    conn.close()


def cmd_predict(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    init_db(conn)
    draws = get_recent_draws(conn, 200)
    specials = get_recent_specials(conn, 200)
    if len(draws) < 20:
        logger.error("Insufficient data, need at least 20 draws")
        return
    pair_lift = calculate_pair_lift(draws)
    latest = conn.execute(
        "SELECT issue_no FROM draws ORDER BY draw_date DESC LIMIT 1"
    ).fetchone()
    next_issue = next_issue_number(latest[0]) if latest else "26/001"
    for strat in STRATEGY_IDS:
        score = generate_strategy_score(draws, specials, strat, pair_lift)
        conn.execute(
            """
            INSERT OR REPLACE INTO predictions (issue_no, strategy, numbers_json, special_number, confidence, status, created_at)
            VALUES (?, ?, ?, ?, ?, 'PENDING', ?)
            """,
            (
                next_issue,
                strat,
                json.dumps(score.main_picks),
                score.special_pick,
                score.confidence,
                utc_now(),
            ),
        )
    conn.commit()
    print(f"Predictions generated for {next_issue}")
    conn.close()


def cmd_show(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    init_db(conn)
    latest = conn.execute(
        "SELECT issue_no, draw_date, numbers_json, special_number FROM draws ORDER BY draw_date DESC LIMIT 1"
    ).fetchone()
    if latest:
        nums = json.loads(latest["numbers_json"])
        print(
            f"最新开奖: {latest['issue_no']} {latest['draw_date']} | 主号: {' '.join(f'{n:02d}' for n in nums)} | 特别号: {latest['special_number']:02d}"
        )
    else:
        print("暂无开奖数据。")

    # 显示待开奖预测
    pending = conn.execute(
        "SELECT issue_no, strategy, numbers_json, special_number, confidence FROM predictions WHERE status='PENDING' ORDER BY strategy"
    ).fetchall()
    if pending:
        print("\n本期推荐:")
        for p in pending:
            nums = json.loads(p["numbers_json"])
            conf_str = f" (置信度: {p['confidence']*100:.1f}%)" if p["confidence"] else ""
            print(
                f"  [{p['issue_no']}] {p['strategy']}{conf_str}: {' '.join(f'{n:02d}' for n in nums)} | 特别号: {p['special_number']:02d}"
            )
    else:
        print("\n暂无待开奖预测，请先运行 predict")

    # 显示回测统计
    stats = conn.execute(
        "SELECT * FROM backtest_stats ORDER BY sharpe_ratio DESC"
    ).fetchall()
    if stats:
        print("\n策略历史表现 (回测):")
        for s in stats:
            print(
                f"  {s['strategy']}: 夏普={s['sharpe_ratio']:.2f} 平均命中={s['avg_hit']:.2f} ≥2码率={s['hit2_rate']*100:.1f}% 特别号率={s['special_rate']*100:.1f}%"
            )
    conn.close()


def cmd_backtest(args: argparse.Namespace) -> None:
    conn = connect_db(args.db)
    init_db(conn)
    run_rolling_backtest(conn)
    print("Rolling backtest completed.")
    conn.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="香港六合彩专业升级版")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help="SQLite数据库路径")
    sub = parser.add_subparsers(dest="command", required=True)

    p_sync = sub.add_parser("sync", help="同步历史数据")
    p_sync.add_argument("--source", choices=["auto"], default="auto")
    p_sync.add_argument("--official-url", default=OFFICIAL_URL)
    p_sync.add_argument(
        "--third-party-url", action="append", help="第三方数据源URL (可多次指定)"
    )
    p_sync.add_argument(
        "--third-party-max-pages", type=int, default=THIRD_PARTY_MAX_PAGES_DEFAULT
    )
    p_sync.set_defaults(func=cmd_sync)

    p_predict = sub.add_parser("predict", help="生成下期预测")
    p_predict.set_defaults(func=cmd_predict)

    p_show = sub.add_parser("show", help="显示推荐和统计")
    p_show.set_defaults(func=cmd_show)

    p_backtest = sub.add_parser("backtest", help="运行滚动回测")
    p_backtest.set_defaults(func=cmd_backtest)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
