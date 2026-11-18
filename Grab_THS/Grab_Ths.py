#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
同花顺(THS)个股主力资金流 -> SQLite  Tip.IndividualFundFlow_THS
================================================================================

前提：数据库和表已经建好（脚本只做结构校验，不执行 DDL）：

    CREATE TABLE "IndividualFundFlow_THS" (
      "TradeDate" DATE NOT NULL,
      "StockCode" TEXT NOT NULL,
      "StockName" TEXT,
      "Price" real,
      "Rise" real,
      "FundFlow_MainForce" real,
      PRIMARY KEY ("TradeDate", "StockCode")
    );

取数天数从 Settings 表读取（键名 Days_THS）：
    INSERT INTO "Settings" VALUES ('Days_THS', '30', 'Grab_THS 项目获取多少天(交易日)的资金流数据');
优先级：命令行 --days > Settings.Days_THS > 默认 30 天。
Settings 表不存在 / 没有 Days_THS / 值非法时，退回默认 30 天并给出提示。

数据源：Tushare Pro  moneyflow_ths（同花顺口径，需 6000 积分，数据自 2019-07-26）

字段对应关系：
    TradeDate          <- trade_date       （20261117 -> 2026-11-17）
    StockCode          <- ts_code          （如 600519.SH）
    StockName          <- name
    Price              <- latest           最新价
    Rise               <- pct_change       涨跌幅（%）
    FundFlow_MainForce <- buy_lg_amount    今日大单净流入额（万元，同花顺口径的「主力」）

关于「主力」口径（重要）：
    同花顺网页上的「主力净流入」= 大单净额（含超大单），对应接口字段 buy_lg_amount；
    而 net_amount 是「大单+中单+小单」的合计，即全部资金净额，不是主力。
    Tushare 接口里 lg+md+sm 恰好等于 net_amount，说明三者是并列的档位拆分，
    因此这里默认取 buy_lg_amount。若你想改用总净额，加 --main-field net_amount 即可。

数值处理：Price、Rise 保留 2 位小数，FundFlow_MainForce 保留 1 位小数
          （四舍五入 ROUND_HALF_UP），NULL 仍写 NULL。

用法：
    python Grab_THS.py                     # 天数按 Settings.Days_THS，默认写 U:\Grab\Tip.db
    python Grab_THS.py --days 5            # 临时覆盖
    python Grab_THS.py --date 20261117     # 只补某一天
    python Grab_THS.py --db "D:\\tmp\\Tip.db"   # 换一个数据库文件

入库时每 500 条打印一次进度（[入库] 500/156631 行（0.3%），耗时 0.1s），
终端下原地刷新，重定向到文件时逐行输出。

依赖：pip install tushare pandas
"""

import argparse
import math
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import tushare as ts

ts.set_token("c37889408c22e87e788a7388c75c99bc9b1fc56d3e1f8bb77cf264ae")   # 或直接 ts.set_token("你的token")
pro = ts.pro_api()

TOKEN = os.getenv("TUSHARE_TOKEN", "").strip()   # 也可直接填 "你的token"
SLEEP = 0.35                    # 逐日请求间隔（秒），避免触发每分钟调用上限
DB_PATH = r"U:\Grab\Tip.db"     # 默认数据库路径（可用 --db 覆盖）
BATCH = 500                     # 入库时每 500 条输出一次进度
TABLE = "IndividualFundFlow_THS"
SETTINGS_TABLE = "Settings"
SETTING_KEY = "Days_THS"
DEFAULT_DAYS = 30               # Settings 缺失时的兜底天数
ROW_LIMIT = 6000                # moneyflow_ths 单次最大返回 6000 条
PRICE_DECIMALS = 2              # Price、Rise 保留 2 位小数
FLOW_DECIMALS = 1               # 资金流保留 1 位小数
_Q_PRICE = Decimal("0.01")
_Q_FLOW = Decimal("0.1")

# FundFlow_MainForce 取哪个字段：buy_lg_amount=大单净额(主力)，net_amount=全部资金净额
MAIN_FIELDS = {"buy_lg_amount": "大单净额（同花顺主力口径）",
               "net_amount": "全部资金净额（大+中+小）"}

REQUIRED_COLUMNS = ["TradeDate", "StockCode", "StockName", "Price", "Rise", "FundFlow_MainForce"]
NUMERIC_COLUMNS = ["Price", "Rise", "FundFlow_MainForce"]

# 幂等写入：同日同股票重复跑不会报错，只更新其余字段
UPSERT = f'''
INSERT INTO "{TABLE}" ("TradeDate", "StockCode", "StockName", "Price", "Rise", "FundFlow_MainForce")
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT("TradeDate", "StockCode") DO UPDATE SET
    "StockName"          = excluded."StockName",
    "Price"              = excluded."Price",
    "Rise"               = excluded."Rise",
    "FundFlow_MainForce" = excluded."FundFlow_MainForce"
'''


# --------------------------------------------------------------------------- #
# Tushare 取数
# --------------------------------------------------------------------------- #
def get_pro(token: str = ""):
    """初始化 pro_api，token 优先级：参数 > 环境变量 > 本机已保存的 token。"""
    token = (token or TOKEN).strip()
    if token:
        ts.set_token(token)
    try:
        return ts.pro_api()
    except Exception as exc:
        raise SystemExit(
            "初始化 Tushare 失败：%s\n"
            "请到 https://tushare.pro/user/token 获取 token 后重试"
            "（可设置环境变量 TUSHARE_TOKEN，或先执行 ts.set_token('xxx')）。" % exc)


def norm_date(value: str) -> str:
    """2026-11-17 / 2026/11/17 / 20261117 -> 20261117。"""
    text = str(value).strip().replace("-", "").replace("/", "")
    datetime.strptime(text, "%Y%m%d")
    return text


def to_db_date(yyyymmdd: str) -> str:
    """20261117 -> 2026-11-17（SQLite 推荐的日期文本格式）。"""
    text = str(yyyymmdd).strip().replace("-", "").replace("/", "")
    return f"{text[:4]}-{text[4:6]}-{text[6:8]}"


def call_ths(pro, **kwargs) -> pd.DataFrame:
    """调用 moneyflow_ths，处理频率限制与积分不足。"""
    params = {k: v for k, v in kwargs.items() if v is not None}
    last_exc = None
    for attempt in range(3):
        try:
            df = pro.moneyflow_ths(**params)
            return df if df is not None else pd.DataFrame()
        except Exception as exc:
            last_exc = exc
            msg = str(exc)
            if "每分钟" in msg:
                time.sleep(61)
                continue
            if "权限" in msg or "积分" in msg:
                raise SystemExit(f"moneyflow_ths 调用失败（该接口需 6000 积分）：{msg}")
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(f"moneyflow_ths 调用失败：{last_exc}")


def get_trade_days(pro, start_date: str, end_date: str) -> list:
    """区间内的交易日（升序）。"""
    cal = pro.trade_cal(exchange="SSE", start_date=start_date, end_date=end_date,
                        fields="cal_date,is_open")
    return sorted(cal.loc[cal["is_open"].astype(int) == 1, "cal_date"].astype(str))


def recent_trade_days(pro, days: int, end_date: str = None) -> list:
    """
    最近 days 个交易日（升序），默认以今天为终点。

    先放宽区间查交易日再截取最后 days 个；春节、国庆长假导致数量不够时
    自动扩大窗口重查，保证恰好 days 个。
    """
    end = norm_date(end_date) if end_date else datetime.now().strftime("%Y%m%d")
    end_dt = datetime.strptime(end, "%Y%m%d")

    all_days = []
    for span in (days * 2 + 10, days * 4 + 30, days * 6 + 90):
        start = (end_dt - timedelta(days=span)).strftime("%Y%m%d")
        all_days = get_trade_days(pro, start, end)
        if len(all_days) >= days:
            break

    return all_days[-days:]


def fetch_one_day(pro, trade_date: str) -> pd.DataFrame:
    """取某一天的全市场数据；空结果表示非交易日或当天数据尚未更新。"""
    return call_ths(pro, trade_date=trade_date)


def fetch_days(pro, dates: list) -> pd.DataFrame:
    """
    逐日拉取全市场并拼接。

    当天（例如盘中或收盘后还没更新）返回空时自动往前补一个交易日，
    保证最终拿到 len(dates) 个「有数据」的交易日。返回结果仍按日期升序。
    """
    dates = list(dates)
    if not dates:
        return pd.DataFrame()

    probe_start = (datetime.strptime(dates[0], "%Y%m%d")
                   - timedelta(days=30)).strftime("%Y%m%d")
    earlier = [d for d in get_trade_days(pro, probe_start, dates[0]) if d < dates[0]]
    pool = earlier + dates                      # 前面预留可回补的交易日

    frames, got = [], 0
    for day in reversed(pool):                  # 从最新往前取
        if got >= len(dates):
            break
        df = fetch_one_day(pro, day)
        if df.empty:
            print(f"  {day} 无数据（未更新或停市），跳过并向前补一天")
        else:
            if len(df) >= ROW_LIMIT:
                print(f"  [提醒] {day} 返回 {len(df)} 行，已达单次上限，可能被截断")
            frames.append(df)
            got += 1
            print(f"  [取数] {day} 取得 {len(df)} 行（已完成 {got}/{len(dates)} 个交易日）")
        time.sleep(SLEEP)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["trade_date", "ts_code"], keep="last")
    return out.sort_values(["trade_date", "ts_code"]).reset_index(drop=True)


# --------------------------------------------------------------------------- #
# 数据库 / 配置
# --------------------------------------------------------------------------- #
def connect(db_path: str) -> sqlite3.Connection:
    """连接已存在的数据库（不会建库建表）。"""
    if not os.path.exists(db_path):
        raise SystemExit(f"找不到数据库文件：{os.path.abspath(db_path)}")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = true")
    return conn


def check_table(conn: sqlite3.Connection) -> None:
    """校验表结构与数值列类型，尽早发现结构漂移。"""
    info = conn.execute(f'PRAGMA table_info("{TABLE}")').fetchall()
    if not info:
        raise SystemExit(f'数据库中不存在表 "{TABLE}"，请先建表。')

    columns = {row[1]: (row[2] or "").upper() for row in info}
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        raise SystemExit(f'表 "{TABLE}" 缺少字段：{", ".join(missing)}')

    numeric_types = ("REAL", "FLOAT", "DOUBLE", "NUMERIC", "INTEGER")
    bad = [c for c in NUMERIC_COLUMNS if columns.get(c) not in numeric_types]
    if bad:
        print(f'[提醒] 以下列不是数值类型，比较/排序会退化成文本：{", ".join(bad)}')
    else:
        print("[信息] 表结构校验通过：6 个字段齐全，3 个数值列均为 REAL")


def read_setting(conn: sqlite3.Connection, name: str):
    """从 Settings 表读一个配置项；表不存在 / 无该项 / 值为空都返回 None。"""
    try:
        row = conn.execute(
            f'SELECT "ItemValue" FROM "{SETTINGS_TABLE}" '
            f'WHERE "ItemName" = ? COLLATE NOCASE', (name,)
        ).fetchone()
    except sqlite3.Error:                       # Settings 表不存在
        return None
    if not row or row[0] is None:
        return None
    value = str(row[0]).strip()
    return value or None


def resolve_days(conn: sqlite3.Connection, cli_days):
    """取数天数优先级：--days > Settings.Days_THS > 默认值。返回 (天数, 来源说明)。"""
    if cli_days is not None:
        if int(cli_days) <= 0:
            raise SystemExit(f"--days 必须是正整数，收到：{cli_days}")
        return int(cli_days), "命令行 --days"

    raw = read_setting(conn, SETTING_KEY)
    if raw is None:
        print(f"[提醒] Settings 表里没有 {SETTING_KEY} 项（或表不存在），使用默认 {DEFAULT_DAYS} 天")
        return DEFAULT_DAYS, "默认值"

    try:
        days = int(float(raw))
    except ValueError:
        print(f'[提醒] Settings.{SETTING_KEY} = "{raw}" 不是有效数字，使用默认 {DEFAULT_DAYS} 天')
        return DEFAULT_DAYS, "默认值"

    if days <= 0:
        print(f'[提醒] Settings.{SETTING_KEY} = "{raw}" 不是正整数，使用默认 {DEFAULT_DAYS} 天')
        return DEFAULT_DAYS, "默认值"

    return days, f'Settings.{SETTING_KEY} = "{raw}"'


# --------------------------------------------------------------------------- #
# 加工 / 入库
# --------------------------------------------------------------------------- #
def _num(value, quant):
    """数值 -> 按 quant 位数四舍五入；None / NaN / 非数字 -> None。"""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    v = float(Decimal(str(f)).quantize(quant, rounding=ROUND_HALF_UP))
    return v + 0.0                          # -0.0 归一成 0.0


def to_rows(df: pd.DataFrame, main_field: str = "buy_lg_amount") -> list:
    """DataFrame -> executemany 用的元组列表，顺序与 UPSERT 一致。"""
    if main_field not in df.columns:
        raise SystemExit(f"接口返回中没有字段 {main_field}，请用 --main-field 指定其他字段")

    rows = []
    for rec in df.itertuples(index=False):
        code = str(rec.ts_code).strip()
        if not code:
            continue
        rows.append((
            to_db_date(rec.trade_date),
            code,
            rec.name if pd.notna(rec.name) else None,
            _num(rec.latest, _Q_PRICE),              # Price
            _num(rec.pct_change, _Q_PRICE),          # Rise
            _num(getattr(rec, main_field), _Q_FLOW),  # FundFlow_MainForce
        ))
    return rows


def upsert(conn: sqlite3.Connection, rows: list, batch: int = BATCH) -> int:
    """
    分批写入同一个事务（中途失败整体回滚），每 batch 条打印一次进度。
    终端下用 \\r 原地刷新，重定向到文件时改为逐行输出。
    """
    total = len(rows)
    if total == 0:
        return 0

    before = conn.total_changes
    done = 0
    tty = sys.stdout.isatty()
    t0 = time.time()
    with conn:                                   # 成功自动 commit，异常自动 rollback
        for i in range(0, total, batch):
            chunk = rows[i:i + batch]
            conn.executemany(UPSERT, chunk)
            done += len(chunk)
            pct = done * 100.0 / total
            msg = f"[入库] {done}/{total} 行（{pct:5.1f}%），耗时 {time.time() - t0:.1f}s"
            if tty:
                print(f"\r{msg}", end="", flush=True)
            else:
                print(msg, flush=True)
    if tty:
        print()                                  # 结束 \r 行
    return conn.total_changes - before


def report(conn: sqlite3.Connection, dates: list) -> None:
    """入库结果自检。dates 用库里实际的 YYYY-MM-DD 文本。"""
    total = conn.execute(f'SELECT COUNT(*) FROM "{TABLE}"').fetchone()[0]
    stocks = conn.execute(f'SELECT COUNT(DISTINCT "StockCode") FROM "{TABLE}"').fetchone()[0]
    dmin, dmax = conn.execute(
        f'SELECT MIN("TradeDate"), MAX("TradeDate") FROM "{TABLE}"').fetchone()
    print(f"\n表内合计 {total} 行，{stocks} 只股票，日期范围 {dmin} ~ {dmax}")

    print("\n本次涉及日期的入库行数：")
    placeholders = ",".join("?" * len(dates))
    q = (f'SELECT "TradeDate", COUNT(*) FROM "{TABLE}" '
         f'WHERE "TradeDate" IN ({placeholders}) '
         f'GROUP BY "TradeDate" ORDER BY "TradeDate"')
    for d, n in conn.execute(q, list(dates)):
        print(f"  {d}  {n} 行")


# --------------------------------------------------------------------------- #
# 入口
# --------------------------------------------------------------------------- #
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="同花顺个股主力资金流入库（moneyflow_ths -> SQLite）")
    p.add_argument("--db", default=DB_PATH,
                   help=f"数据库文件，默认 {DB_PATH}")
    p.add_argument("--days", type=int, default=None,
                   help=f"临时覆盖 Settings.{SETTING_KEY}；不传则读 Settings")
    p.add_argument("--date", default=None, help="只取某一天，如 20261117")
    p.add_argument("--end", default=None, help="倒数天数的截止日，默认今天")
    p.add_argument("--token", default=None, help="Tushare token，默认读环境变量 TUSHARE_TOKEN")
    p.add_argument("--main-field", choices=sorted(MAIN_FIELDS), default="buy_lg_amount",
                   help="FundFlow_MainForce 取哪个字段：" +
                        "；".join(f"{k}={v}" for k, v in MAIN_FIELDS.items()))
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    pro = get_pro(args.token or "")

    conn = connect(args.db)
    try:
        check_table(conn)

        # -------- 确定交易日 --------
        if args.date:
            dates = [norm_date(args.date)]
        else:
            days, source = resolve_days(conn, args.days)
            print(f"[信息] 取数天数：{days}（来源：{source}）")
            dates = recent_trade_days(pro, days, args.end)
            if not dates:
                print("[提示] 未取到交易日，请检查日期范围")
                return 0
        print(f"计划取 {len(dates)} 个交易日：{dates[0]} ~ {dates[-1]}（全市场）")

        # -------- 取数（单日失败自动向前补一天） --------
        raw = fetch_one_day(pro, dates[0]) if len(dates) == 1 else fetch_days(pro, dates)
        if raw.empty:
            print("[提示] 接口返回空：可能是非交易日，或数据尚未更新")
            return 0

        rows = to_rows(raw, args.main_field)
        used = sorted({to_db_date(d) for d in raw["trade_date"].astype(str).unique()})
        print(f"\n实际取到 {len(used)} 个交易日：{used[0]} ~ {used[-1]}")
        print(f"待写入 {len(rows)} 行（Price/Rise {PRICE_DECIMALS} 位小数，"
              f"资金流 {FLOW_DECIMALS} 位小数，主力取 {args.main_field}），"
              f"数据库：{os.path.abspath(args.db)}")

        # -------- 入库 --------
        changed = upsert(conn, rows)
        print(f"[完成] 写入/更新 {changed} 行")
        report(conn, used)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())