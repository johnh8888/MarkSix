import { prisma } from "@/lib/prisma";
import { type CsvDrawRecord } from "@/lib/types";

function sortAndDedupe(records: CsvDrawRecord[]): CsvDrawRecord[] {
  const merged = new Map<string, CsvDrawRecord>();

  for (const record of records) {
    merged.set(record.issueNo, record);
  }

  return [...merged.values()].sort((a, b) => a.drawDate.getTime() - b.drawDate.getTime());
}

function resolveSource(record: CsvDrawRecord): string {
  return record.source ?? "unknown";
}

export async function upsertDrawRecords(records: CsvDrawRecord[]) {
  const merged = sortAndDedupe(records);
  let inserted = 0;
  let updated = 0;
  let latestIssue = "";

  for (const record of merged) {
    const numbersJson = JSON.stringify(record.numbers);
    const source = resolveSource(record);
    const existing = await prisma.draw.findUnique({
      where: { issueNo: record.issueNo },
      select: {
        drawDate: true,
        numbersJson: true,
        specialNumber: true,
        source: true,
      },
    });

    if (!existing) {
      await prisma.draw.create({
        data: {
          issueNo: record.issueNo,
          drawDate: record.drawDate,
          numbersJson,
          specialNumber: record.specialNumber,
          source,
        },
      });
      inserted += 1;
      latestIssue = record.issueNo;
      continue;
    }

    const changed =
      existing.drawDate.getTime() !== record.drawDate.getTime() ||
      existing.numbersJson !== numbersJson ||
      existing.specialNumber !== record.specialNumber ||
      existing.source !== source;

    if (changed) {
      await prisma.draw.update({
        where: { issueNo: record.issueNo },
        data: {
          drawDate: record.drawDate,
          numbersJson,
          specialNumber: record.specialNumber,
          source,
        },
      });
      updated += 1;
    }

    latestIssue = record.issueNo;
  }

  return {
    totalRecords: merged.length,
    inserted,
    updated,
    latestIssue,
  };
}
