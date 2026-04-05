import { prisma } from "@/lib/prisma";
import { describeSpecialNumber, formatNumber, inferYearFromIssue } from "@/lib/marksix";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const PAGE_SIZE = 50;

function parseJsonArray(text: string): number[] {
  try {
    return JSON.parse(text) as number[];
  } catch {
    return [];
  }
}

function formatDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}

function parsePage(value?: string): number {
  const page = Number(value);
  if (!Number.isInteger(page) || page < 1) {
    return 1;
  }
  return page;
}

function buildPageHref(page: number): string {
  return page <= 1 ? "/history" : `/history?page=${page}`;
}

export default async function HistoryPage({
  searchParams,
}: {
  searchParams?: Promise<Record<string, string | string[] | undefined>>;
}) {
  const resolvedSearchParams = (await searchParams) ?? {};
  const pageParam = resolvedSearchParams.page;
  const currentPage = parsePage(Array.isArray(pageParam) ? pageParam[0] : pageParam);

  const [totalDraws, oldestDraw, newestDraw] = await Promise.all([
    prisma.draw.count(),
    prisma.draw.findFirst({
      orderBy: { drawDate: "asc" },
      select: { issueNo: true, drawDate: true },
    }),
    prisma.draw.findFirst({
      orderBy: { drawDate: "desc" },
      select: { issueNo: true, drawDate: true },
    }),
  ]);

  const totalPages = Math.max(1, Math.ceil(totalDraws / PAGE_SIZE));
  const safePage = Math.min(currentPage, totalPages);
  const latestDraws = await prisma.draw.findMany({
    orderBy: { drawDate: "desc" },
    skip: (safePage - 1) * PAGE_SIZE,
    take: PAGE_SIZE,
  });

  const startIndex = totalDraws === 0 ? 0 : (safePage - 1) * PAGE_SIZE + 1;
  const endIndex = totalDraws === 0 ? 0 : Math.min(safePage * PAGE_SIZE, totalDraws);
  const pageWindowStart = Math.max(1, safePage - 2);
  const pageWindowEnd = Math.min(totalPages, safePage + 2);
  const visiblePages = Array.from(
    { length: pageWindowEnd - pageWindowStart + 1 },
    (_, index) => pageWindowStart + index,
  );

  return (
    <section className="stack">
      <div className="section-head">
        <div>
          <p className="eyebrow">History</p>
          <h2>历史开奖数据</h2>
        </div>
        <p className="kv section-copy">展示数据库中的全部历史开奖，按每页 50 条分页，便于核对同步结果与特别号码走势。</p>
      </div>

      <div className="stat-grid">
        <article className="card stat-card">
          <p className="kv">总记录数</p>
          <h3 className="issue-title">{totalDraws}</h3>
          <p className="kv">当前第 {safePage} / {totalPages} 页</p>
        </article>
        <article className="card stat-card">
          <p className="kv">最早一期</p>
          <h3>{oldestDraw?.issueNo ?? "-"}</h3>
          <p className="kv">{oldestDraw ? formatDate(oldestDraw.drawDate) : "-"}</p>
        </article>
        <article className="card stat-card">
          <p className="kv">最近一期</p>
          <h3>{newestDraw?.issueNo ?? "-"}</h3>
          <p className="kv">{newestDraw ? formatDate(newestDraw.drawDate) : "-"}</p>
        </article>
      </div>

      {latestDraws.length > 0 ? (
        <div className="card">
          <div className="card-head">
            <div>
              <h3>历史开奖列表</h3>
              <p className="kv">当前显示第 {startIndex} - {endIndex} 条，共 {totalDraws} 条</p>
            </div>
            <span className="badge">50 条 / 页</span>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>期号</th>
                  <th>日期</th>
                  <th>正码</th>
                  <th>特别号</th>
                  <th>来源</th>
                </tr>
              </thead>
              <tbody>
                {latestDraws.map((draw) => {
                  const numbers = parseJsonArray(draw.numbersJson);
                  const year = inferYearFromIssue(draw.issueNo, draw.drawDate.getUTCFullYear());

                  return (
                    <tr key={draw.id}>
                      <td>{draw.issueNo}</td>
                      <td>{formatDate(draw.drawDate)}</td>
                      <td>
                        <div className="history-balls">
                          {numbers.map((number) => (
                            <span key={number} className="history-ball">
                              {formatNumber(number)}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td>{describeSpecialNumber(draw.specialNumber, year)}</td>
                      <td>
                        <span className="source-chip">{draw.source ?? "-"}</span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <div className="pagination">
            <a
              href={safePage > 1 ? buildPageHref(safePage - 1) : undefined}
              className={`page-link ${safePage <= 1 ? "is-disabled" : ""}`}
              aria-disabled={safePage <= 1}
            >
              上一页
            </a>

            <div className="page-list">
              {visiblePages.map((page) => (
                <a
                  key={page}
                  href={buildPageHref(page)}
                  className={`page-link ${page === safePage ? "is-active" : ""}`}
                  aria-current={page === safePage ? "page" : undefined}
                >
                  {page}
                </a>
              ))}
            </div>

            <a
              href={safePage < totalPages ? buildPageHref(safePage + 1) : undefined}
              className={`page-link ${safePage >= totalPages ? "is-disabled" : ""}`}
              aria-disabled={safePage >= totalPages}
            >
              下一页
            </a>
          </div>
        </div>
      ) : (
        <div className="empty-state">
          <p>当前数据库还没有历史记录。</p>
          <p className="kv">先调用 <code>/api/jobs/sync-latest</code> 完成初始化。</p>
        </div>
      )}
    </section>
  );
}
