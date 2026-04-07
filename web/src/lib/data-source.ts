import { parseDrawCsv, readLocalCsv } from "@/lib/csv";
import { loadLottolyzerRecords } from "@/lib/lottolyzer-source";
import { loadMarksix6Records } from "@/lib/marksix6-source";
import { loadOfficialRecords } from "@/lib/official-source";
import { type CsvDrawRecord } from "@/lib/types";

function sortRecords(records: Iterable<CsvDrawRecord>): CsvDrawRecord[] {
  return [...records].sort((a, b) => a.drawDate.getTime() - b.drawDate.getTime());
}

function mergeRecordSets(recordSets: CsvDrawRecord[][]): CsvDrawRecord[] {
  const merged = new Map<string, CsvDrawRecord>();

  for (const set of recordSets) {
    for (const record of set) {
      merged.set(record.issueNo, record);
    }
  }

  return sortRecords(merged.values());
}

async function loadRemoteCsvRecords(): Promise<CsvDrawRecord[]> {
  const remoteCsv = process.env.RESULT_CSV_URL?.trim();
  if (!remoteCsv) {
    return [];
  }

  const response = await fetch(remoteCsv, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to fetch RESULT_CSV_URL: ${response.status}`);
  }

  const raw = await response.text();
  return parseDrawCsv(raw).map((record) => ({
    ...record,
    source: "remote_csv",
  }));
}

function loadLocalSeedRecords(): CsvDrawRecord[] {
  const configured = process.env.LOCAL_RESULT_CSV_PATH?.trim();
  const candidates = [
    configured,
    "./Mark_Six.csv",
  ].filter((value, index, arr): value is string => Boolean(value) && arr.indexOf(value) === index);

  let lastError: Error | null = null;

  for (const filePath of candidates) {
    try {
      const source = filePath.includes("/web/") || filePath.startsWith("./web/")
        ? "local_web_csv"
        : "local_csv";
      return parseDrawCsv(readLocalCsv(filePath)).map((record) => ({
        ...record,
        source,
      }));
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
    }
  }

  throw lastError ?? new Error("No local CSV source is available");
}

async function loadOptionalSource(
  loader: () => Promise<CsvDrawRecord[]> | CsvDrawRecord[],
  required: boolean,
): Promise<CsvDrawRecord[]> {
  try {
    return await loader();
  } catch (error) {
    if (required) {
      throw error;
    }
    return [];
  }
}

export async function loadDrawRecords(): Promise<CsvDrawRecord[]> {
  const provider = (process.env.RESULT_PROVIDER || "hybrid").trim().toLowerCase();
  const marksix6Required = (process.env.MARKSIX6_SOURCE_REQUIRED || "").trim() === "true";
  const officialRequired = (process.env.OFFICIAL_SOURCE_REQUIRED || "").trim() === "true";
  const lottolyzerRequired = (process.env.LOTTOLYZER_SOURCE_REQUIRED || "").trim() === "true";

  if (provider === "csv") {
    const remote = await loadOptionalSource(loadRemoteCsvRecords, false);
    if (remote.length > 0) {
      return remote;
    }

    return loadOptionalSource(loadLocalSeedRecords, true);
  }

  if (provider === "official") {
    return loadOptionalSource(loadOfficialRecords, true);
  }

  if (provider === "marksix6") {
    return loadOptionalSource(loadMarksix6Records, true);
  }

  if (provider === "lottolyzer") {
    return loadOptionalSource(loadLottolyzerRecords, true);
  }

  const local = await loadOptionalSource(loadLocalSeedRecords, false);
  const remote = await loadOptionalSource(loadRemoteCsvRecords, false);
  const marksix6 = await loadOptionalSource(loadMarksix6Records, marksix6Required);
  const official = await loadOptionalSource(loadOfficialRecords, officialRequired);
  const lottolyzer = await loadOptionalSource(loadLottolyzerRecords, lottolyzerRequired);
  const merged = mergeRecordSets([local, remote, marksix6, official, lottolyzer]);

  if (merged.length === 0) {
    throw new Error("No draw records were loaded from any configured source");
  }

  return merged;
}
