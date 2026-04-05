import { type CsvDrawRecord } from "@/lib/types";

const DEFAULT_BASE_URL = "https://zh.lottolyzer.com/history/hong-kong/mark-six";

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function decodeHtml(html: string): string {
  return html
    .replace(/&nbsp;|&#160;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&quot;/gi, "\"")
    .replace(/&#39;/gi, "'")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">");
}

function normalizeTextLines(html: string): string[] {
  const text = decodeHtml(
    html
      .replace(/<script[\s\S]*?<\/script>/gi, "\n")
      .replace(/<style[\s\S]*?<\/style>/gi, "\n")
      .replace(/<\/(tr|p|div|li|section|article|table|thead|tbody|tfoot|h\d)>/gi, "\n")
      .replace(/<br\s*\/?>/gi, "\n")
      .replace(/<[^>]+>/g, " "),
  );

  return text
    .split(/\r?\n/)
    .map((line) => line.replace(/\s+/g, " ").trim())
    .filter(Boolean);
}

function parseSpecialSummaryLine(line: string): CsvDrawRecord | null {
  if (!/^\d{2}\/\d{3}\s+\d{4}-\d{2}-\d{2}\s+/.test(line)) {
    return null;
  }

  const normalized = line.replace(/(\d)\s*\/\s*(\d)/g, "$1/$2");
  const tokens = normalized.split(/\s+/);

  if (tokens.length < 13) {
    return null;
  }

  const issueNo = tokens[0];
  const drawDate = new Date(`${tokens[1]}T12:00:00Z`);
  const numbers = tokens[2]
    .split(",")
    .map((value) => Number(value))
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 49);
  const specialNumber = Number(tokens[3]);
  const remaining = tokens.slice(4);
  const featureTokens = remaining.slice(-9);

  if (
    Number.isNaN(drawDate.getTime()) ||
    numbers.length !== 6 ||
    !Number.isInteger(specialNumber) ||
    specialNumber < 1 ||
    specialNumber > 49 ||
    featureTokens.length !== 9
  ) {
    return null;
  }

  return {
    issueNo,
    drawDate,
    numbers,
    specialNumber,
    source: "lottolyzer_summary",
  };
}

function buildPageUrl(page: number, perPage: number): string {
  const baseUrl = (process.env.LOTTOLYZER_HISTORY_URL || DEFAULT_BASE_URL).trim().replace(/\/+$/, "");
  return `${baseUrl}/page/${page}/per-page/${perPage}/summary-view`;
}

function mergeRecords(recordSets: CsvDrawRecord[][]): CsvDrawRecord[] {
  const merged = new Map<string, CsvDrawRecord>();

  for (const set of recordSets) {
    for (const record of set) {
      merged.set(record.issueNo, record);
    }
  }

  return [...merged.values()].sort((a, b) => a.drawDate.getTime() - b.drawDate.getTime());
}

export async function loadLottolyzerRecords(): Promise<CsvDrawRecord[]> {
  const perPage = clamp(Number(process.env.LOTTOLYZER_PER_PAGE || "50"), 10, 50);
  const maxPages = clamp(Number(process.env.LOTTOLYZER_MAX_PAGES || "1"), 1, 5);
  const recordSets: CsvDrawRecord[][] = [];

  for (let page = 1; page <= maxPages; page += 1) {
    const response = await fetch(buildPageUrl(page, perPage), {
      cache: "no-store",
      headers: {
        "user-agent": "Mozilla/5.0 (compatible; marksix-predictor/1.0)",
        accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });

    if (!response.ok) {
      if (page === 1) {
        throw new Error(`Failed to fetch Lottolyzer history page: ${response.status}`);
      }
      break;
    }

    const html = await response.text();
    const records = normalizeTextLines(html)
      .map(parseSpecialSummaryLine)
      .filter((record): record is CsvDrawRecord => Boolean(record));

    if (records.length === 0) {
      if (page === 1) {
        throw new Error("Lottolyzer history page parsed 0 draw records");
      }
      break;
    }

    recordSets.push(records);
  }

  if (recordSets.length === 0) {
    throw new Error("No records loaded from Lottolyzer");
  }

  return mergeRecords(recordSets);
}
