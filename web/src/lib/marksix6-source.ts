import { type CsvDrawRecord } from "@/lib/types";

type Marksix6Payload = Record<string, unknown>;

function toInt(value: unknown): number | null {
  const text = `${value ?? ""}`.trim();
  if (!text) {
    return null;
  }

  const parsed = Number(text);
  if (!Number.isInteger(parsed)) {
    return null;
  }

  return parsed;
}

function normalizeIssueNo(input: unknown): string | null {
  const raw = `${input ?? ""}`.trim();
  if (!raw) {
    return null;
  }

  if (/^\d{2}\/\d{3}$/.test(raw)) {
    return raw;
  }

  const digits = raw.replace(/\D/g, "");
  if (/^\d{7}$/.test(digits)) {
    return `${digits.slice(2, 4)}/${digits.slice(4)}`;
  }

  return null;
}

function normalizeDate(input: unknown): Date | null {
  const raw = `${input ?? ""}`.trim();
  if (!raw) {
    return null;
  }

  const normalized = raw.includes("T") ? raw : raw.replace(" ", "T");
  const withTimezone = /Z$|[+-]\d{2}:\d{2}$/.test(normalized) ? normalized : `${normalized}+08:00`;
  const date = new Date(withTimezone);
  return Number.isNaN(date.getTime()) ? null : date;
}

function parseNumbers(payload: Marksix6Payload): number[] {
  const fromList = Array.isArray(payload.numbers)
    ? payload.numbers
      .map(toInt)
      .filter((value): value is number => value !== null && value >= 1 && value <= 49)
    : [];

  if (fromList.length >= 7) {
    return fromList.slice(0, 7);
  }

  const openCode = `${payload.openCode ?? ""}`.trim();
  if (!openCode) {
    return [];
  }

  return openCode
    .split(/[^\d]+/)
    .filter(Boolean)
    .map((value) => Number(value))
    .filter((value) => Number.isInteger(value) && value >= 1 && value <= 49);
}

export async function loadMarksix6Records(): Promise<CsvDrawRecord[]> {
  const url =
    process.env.MARKSIX6_API_URL?.trim() ||
    "https://api3.marksix6.net/lottery_api.php?type=hk";

  const response = await fetch(url, {
    cache: "no-store",
    headers: {
      "user-agent": "Mozilla/5.0 (compatible; marksix-predictor/1.0)",
      accept: "application/json,text/plain,*/*",
    },
  });

  if (!response.ok) {
    throw new Error(`Failed to fetch MARKSIX6_API_URL: ${response.status}`);
  }

  const payload = (await response.json()) as Marksix6Payload;
  const issueNo = normalizeIssueNo(payload.expect ?? payload.issueNo ?? payload.issue ?? payload.drawNo);
  const drawDate = normalizeDate(payload.openTime ?? payload.drawDate ?? payload.date);
  const values = parseNumbers(payload);

  if (!issueNo || !drawDate || values.length < 7) {
    throw new Error("Marksix6 source parsed incomplete record");
  }

  return [
    {
      issueNo,
      drawDate,
      numbers: values.slice(0, 6),
      specialNumber: values[6],
      source: "marksix6_api",
    },
  ];
}
