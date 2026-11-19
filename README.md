# 基金自算实时估值（MVP）

多用户网页工具：每个用户有自己的基金列表，盘中按基金最新披露的前十大重仓股实时涨跌，自算基金估值。

## 架构

- 后端：Python 3 + FastAPI + uvicorn（单进程，监听 127.0.0.1:8000）
- 数据库：SQLite（WAL），存用户、会话、自选列表、持仓缓存、净值缓存
- 前端：单文件 HTML + 原生 JS，FastAPI 直接托管，无构建步骤，手机/电脑自适应
- 缓存：股票行情内存缓存 45 秒（全站共用，一次批量请求）；净值缓存 1 小时；持仓缓存 24 小时；上游失败时返回上次缓存并标记 stale

## 数据源（2026-11-19 实测可用）

| 用途 | 接口 | 备注 |
|---|---|---|
| 基金搜索 | fundsuggest.eastmoney.com/FundSearchAPI.ashx | 需 User-Agent |
| 官方净值 | api.fund.eastmoney.com/f10/lsjz | 需 User-Agent + Referer（jjjz_{code}.html） |
| 前十大持仓 | fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc | 返回 HTML 表格，需解析；含报告期和截止日期 |
| 股票实时行情 | hq.sinajs.cn/list= | 需 Referer: finance.sina.com.cn，GBK 编码，支持批量 |

新浪行情字段（实测）：
- A 股（sh/sz）：0=名称，2=昨收，3=现价，30=日期，31=时间
- 港股（hk）：0=英文名，1=中文名，3=昨收，6=现价，17=日期，18=时间

股票代码映射：6 位且 6/9 开头 → sh；6 位其他 → sz；5 位数字 → hk；字母代码（美股）MVP 跳过并在结果中标注 skipped_holdings。

## 估值方法

- 估算涨跌幅 = Σ（重仓股占净值比例 × 该股今日涨跌幅）/ 100
- 自算估值 = 最新官方单位净值 ×（1 + 估算涨跌幅 / 100）
- 每条估值同时展示：持仓覆盖率（前十大合计占比）、持仓报告期、行情时间

局限：只披露前十大（通常覆盖约一半仓位），持仓为季报数据、基金经理调仓后会偏；债券/现金部分按 0 涨跌；港股按港币行情直接计算（未计汇率变动）。页面标注：自算估值，仅供参考，以基金公司公布净值为准。

## 本地运行

```bash
cd fund-valuation
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
./venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000
# 浏览器打开 http://127.0.0.1:8000
```

## VPS 部署（Debian 13）

```bash
# 把整个 fund-valuation 目录传到 VPS 后：
cd fund-valuation
sudo bash install.sh
curl -s http://127.0.0.1:8000/api/me   # 应返回未登录提示
```

服务只监听 127.0.0.1:8000。对外访问请在前面加 Nginx 或 Caddy 反代并配 HTTPS，例如 Caddyfile：

```
your.domain {
    reverse_proxy 127.0.0.1:8000
}
```

备份只需复制 `fund.db` 文件。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | /api/register | 注册 {username, password} |
| POST | /api/login | 登录，返回 HttpOnly 会话 Cookie |
| POST | /api/logout | 退出 |
| GET | /api/me | 当前用户 |
| GET | /api/search?q= | 基金搜索（需登录） |
| GET | /api/watchlist | 我的基金列表 |
| POST | /api/watchlist | 添加基金 {fund_code}，代码须 6 位数字 |
| DELETE | /api/watchlist/{id} | 删除（仅本人记录） |
| GET | /api/estimates | 列表 + 自算估值 |

所有自选列表操作都按会话中的 user_id 过滤，不信任前端传入的用户 ID。

## 文件

- `app.py` 后端全部逻辑
- `static/index.html` 前端
- `requirements.txt` fastapi / uvicorn / httpx
- `install.sh` Debian 13 一键安装（venv + systemd 服务）
