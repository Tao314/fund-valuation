# fund-valuation 最小测试护栏

标准库 `unittest`，不加任何新依赖（`requirements.txt` 不动，生产安装流程不变）。

## 一条命令跑全套

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python -m unittest discover -s tests -v
```

`requirements.txt` 里的 FastAPI/uvicorn/httpx 只是为了能 import `app.py`
和跑启动冒烟，测试本身只用标准库。

## 覆盖范围

| 文件 | 覆盖 |
| --- | --- |
| `test_truenav.py` | TrueNAV 核心回归：实时估值加权、未披露残差补齐、分钟曲线、OLS β 校准（手算值钉死）、可信度分档、日涨跌 |
| `test_dates.py` | 日期判断边界：A 股开盘时段、交易窗口、美股时段、最近收盘日 |
| `test_rank.py` | 排行榜规范化：`parse_rank_rows` 缺字段/坏数字/短行、入榜前过滤、页级容错（后页失败出局部榜、首页失败仍报错） |
| `test_fallback.py` | 数据源降级：200 空数据/坏 JSON 判失败、失败有记录、东财挂了切 Yahoo 日线、全挂返回旧缓存 |
| `test_smoke.py` | 启动冒烟：临时拷贝里起 uvicorn，`/` 200、`/api/config` 返回当前版本、未登录 `/api/search` 401 |

## 安全

- 所有测试只用临时库/临时拷贝，**不碰 `fund.db`、不碰生产 VPS、不联网调真实上游**（上游一律 mock）。
- `import app` 会在仓库根目录建一个空 `fund.db`（`init_db()` 在模块级执行，`fund.db` 在 `.gitignore` 里）；测试退出时自动删掉。

## 已知缺口（测试钉住、未修复）

- **没有节假日日历**：`_cn_market_open` 只看星期几，工作日节假日（如 2026-10-01 国庆）会被当成交易日。`test_dates.py` 里有一处 `@unittest.expectedFailure` 钉住这一点；补日历那天去掉装饰器，测试会响一声提醒。
- Yahoo 仍是港美股的最后兜底；排行榜仍无独立第二数据源（只有东财体系内页级容错）。
