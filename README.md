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
- 美股（gb_）：0=名称，1=现价，2=涨跌幅%，3=北京时间，26=昨收

股票代码映射：6 位且 6/9 开头 → sh；6 位其他 → sz；5 位数字 → hk；字母代码（美股）→ gb_ + 小写代码（如 NVDA → gb_nvda）。行情时间取全部持仓中最新的一条（QDII 基金在美股时段显示美股实时时间）。

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

## 安装（VPS，Debian 13）

```bash
git clone https://github.com/Tao314/fund-valuation.git /opt/fund-valuation
cd /opt/fund-valuation
sudo bash install.sh
curl -s http://127.0.0.1:8000/api/me   # 应返回 {"detail":"未登录"}
```

install.sh 会做：装 python3/venv、建虚拟环境装依赖、写 systemd 服务 `fund-valuation`（监听 127.0.0.1:8000）并启动。脚本可重复执行（幂等）。

服务只监听 127.0.0.1:8000。对外访问请在前面加 Nginx 或 Caddy 反代并配 HTTPS，例如 Caddyfile：

```
your.domain {
    reverse_proxy 127.0.0.1:8000
}
```

更新到新版：

```bash
cd /opt/fund-valuation
git pull
systemctl restart fund-valuation
```

常用运维命令：

```bash
systemctl status fund-valuation        # 看状态
journalctl -u fund-valuation -n 30 -l  # 看日志
```

备份只需复制 `fund.db` 文件。

## 卸载

```bash
systemctl disable --now fund-valuation
rm -f /etc/systemd/system/fund-valuation.service
systemctl daemon-reload
rm -rf /opt/fund-valuation
```

删完即干净。install.sh 装的 python3、python3-venv 等系统包可保留，不影响其他服务；反代（Nginx/Caddy）里的站点配置需自行删除。

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
| POST | /api/watchlist/reorder | 在单个分类内重排 {category, ids: [id, ...]}，ids 须与该分类全部基金一致，否则 400；不影响分类顺序 |
| POST | /api/categories/reorder | 重排分类顺序 {names: [分类名, ...]}，基金随分类整组移动 |
| POST | /api/categories | 新建分类 {name}，1-20 字，已存在则 400；排到分类顺序末尾 |
| POST | /api/categories/rename | 重命名分类 {old, new}，new 1-20 字；old 不存在 404，new 已存在 400；同步更新该分类下基金的分类名 |
| POST | /api/categories/delete | 删除分类 {name}，不存在 404；该分类下基金变为未分类 |
| POST | /api/watchlist/{id}/category | 设置分类 {category}，最长 20 字，空 = 未分类；新分类自动登记到分类顺序末尾，基金移到该分类末尾 |
| GET | /api/settings | 当前用户设置 {refresh_seconds} |
| POST | /api/settings | 保存自动刷新间隔 {refresh_seconds}，须 10-600 秒，否则 400 |
| POST | /api/change-password | 修改密码 {old_password, new_password}，新密码至少 6 位；旧密码错误 400；成功后其他会话失效，当前会话保留 |
| GET | /api/estimates | 列表 + 自算估值（含 category 字段、categories 分类顺序数组和 refresh_seconds 自动刷新间隔） |

排序规则：先按分类顺序（user_categories 表，未分类永远最后），分类内按基金 sort_order。调基金顺序只在所属分类内生效，不动分类顺序；调分类顺序整组移动，组内基金顺序不变。

页面分两个视图（顶部「估值」/「设置」切换）：
- 估值页：只看估值表格（按分类分组、组标题只有分类名）、刷新按钮、自动刷新倒计时和更新标记说明，没有管理控件。
- 设置页：自动刷新间隔（10-600 秒，保存后立即生效）、分类管理（新增/重命名/删除/↑↓ 排序）、自选基金（搜索添加、设分类、组内 ↑↓、删除）、账户（修改密码）。

自动刷新间隔按用户存数据库（users.refresh_seconds，默认 60），倒计时跟随该值；本次刷新有变化的基金行黄底高亮并带「更新」标记，无变化时标记自动消失。

所有自选列表操作都按会话中的 user_id 过滤，不信任前端传入的用户 ID。

## 文件

- `app.py` 后端全部逻辑
- `static/index.html` 前端
- `requirements.txt` fastapi / uvicorn / httpx
- `install.sh` Debian 13 一键安装（venv + systemd 服务）
