# 香港六合彩特别号码预测项目

这是一个可部署到 Vercel 的 Next.js 应用，核心目标是预测“下期特别号码候选池”，而不是 6 个正码。

## 当前能力
- 以 `Mark_Six.csv` 作为本地历史种子数据
- 自动补齐线上最新历史，支持：
  - [Lottolyzer 历史页](https://zh.lottolyzer.com/history/hong-kong/mark-six/page/1/per-page/50/summary-view)
  - [HKJC 最近 30 期 JSON](https://bet.hkjc.com/contentserver/jcbw/cmc/last30draw.json)
- Vercel Cron 自动同步最新期次，并写入 Postgres
- 自动复盘特别号码是否命中候选池
- 4 套特别号码预测方案：
  - `zodiac_special_v1` 生肖号码方案
  - `hot_special_v1` 热门号码方案
  - `cold_special_v1` 冷门号码方案
  - `knowledge_mix_v1` 其他方案

## 特别号码预测方案
1. 生肖号码方案
   基于特别号生肖热度、生肖遗漏和生肖转移节奏，输出下期更可能出现的生肖对应号码，控制在 30 个以内。
2. 热门号码方案
   基于近期特别号频率、主号带动效应和相邻期开奖接力特征，输出热门特别号候选池。
3. 冷门号码方案
   基于长遗漏、低热度、分区缺口和波色缺口，输出具回补潜力的冷门特别号候选池。
4. 其他方案
   综合热度、冷门、生肖、波色、分区和主号联动，形成平衡型特别号候选池。

## 数据策略
- 本地初始化优先读取 `Mark_Six.csv`
- 自动同步默认使用 `RESULT_PROVIDER=hybrid`
- `hybrid` 模式会合并：
  - 本地 CSV
  - 远程 CSV（如配置了 `RESULT_CSV_URL`）
  - HKJC 官方 JSON
  - Lottolyzer 历史页
- 同一期号按 `issueNo` 去重，较新的远程来源会覆盖本地旧记录

说明：
- `Mark_Six.csv` 不需要把所有字段都写进数据库，目前数据库只持久化预测真正需要的核心字段：
  - `issueNo`
  - `drawDate`
  - `numbersJson`
  - `specialNumber`
  - `source`
- 其他统计维度如生肖、波色、分区、冷热和主号联动在预测时动态计算。

## 本地启动
1. 安装依赖
```bash
npm install
```

2. 配置环境变量
```bash
cp .env.example .env
```

3. 初始化数据库
```bash
npx prisma generate
npx prisma db push
```

4. 导入历史数据
```bash
npm run bootstrap:history
```

5. 启动项目
```bash
npm run dev
```

## API
- `GET /api/jobs/sync-latest`
  - 功能：同步历史 + 复盘最新已开奖期 + 生成下一期特别号预测
  - 认证：支持 `Authorization: Bearer <CRON_SECRET>` 或 `x-cron-secret: <CRON_SECRET>`
- `POST /api/predictions/generate`
  - 功能：手动生成某一期或下一期特别号预测
  - 请求体示例：
```json
{
  "issueNo": "26/037",
  "strategies": ["zodiac_special_v1", "hot_special_v1"]
}
```

## Vercel 必需变量
```env
DATABASE_URL="auto-injected-by-vercel-neon"
CRON_SECRET="replace-with-a-long-random-string"
RESULT_PROVIDER="hybrid"
LOCAL_RESULT_CSV_PATH="./Mark_Six.csv"
OFFICIAL_RESULT_URL="https://bet.hkjc.com/contentserver/jcbw/cmc/last30draw.json"
LOTTOLYZER_HISTORY_URL="https://zh.lottolyzer.com/history/hong-kong/mark-six"
```

说明：
- `DATABASE_URL` 由 Neon 集成自动注入，一般不用手填。
- 其他 5 个变量建议在 Vercel 项目里手动添加到 `Production` 和 `Preview`。

## 部署到 Vercel
1. 推送代码到 GitHub。
2. 在 Vercel 导入仓库，并把 Root Directory 设为 `web`。
3. 在项目的 `Storage` 或 `Marketplace` 中安装 Neon，并创建数据库。
4. 确认 `DATABASE_URL` 已被 Neon 自动注入。
5. 在项目 `Settings -> Environment Variables` 中新增：
   - `CRON_SECRET`
   - `RESULT_PROVIDER=hybrid`
   - `LOCAL_RESULT_CSV_PATH=./Mark_Six.csv`
   - `OFFICIAL_RESULT_URL=https://bet.hkjc.com/contentserver/jcbw/cmc/last30draw.json`
   - `LOTTOLYZER_HISTORY_URL=https://zh.lottolyzer.com/history/hong-kong/mark-six`
6. Build Command 使用：
```bash
npm run vercel-build
```
7. 首次部署后手动触发一次：
   - `GET /api/jobs/sync-latest`
8. 后续由 `vercel.json` 中的 cron 自动执行。

## 历史补录
如果你有更早的 CSV 文件，可放到 `data/history/` 后执行：
```bash
npm run backfill:history -- --path ./data/history --from-year 1993 --to-year 2007
```

完成后可审计：
```bash
npm run audit:history
```
