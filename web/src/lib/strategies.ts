import { type Draw } from "@prisma/client";
import {
  ALL_NUMBERS,
  ZODIAC_SEQUENCE,
  decodeDrawNumbers,
  getNumbersForZodiac,
  getWaveColor,
  getYearZodiac,
  getZodiacForNumber,
  getZoneIndex,
  inferYearFromIssue,
} from "@/lib/marksix";
import { type StrategyId, type StrategyResult } from "@/lib/types";

type NumberMap = Map<number, number>;
type StringMap = Map<string, number>;

const RECENT_SPECIAL_PENALTY = new Set([1, 2]);

function createNumberMap(defaultValue = 0): NumberMap {
  return new Map(ALL_NUMBERS.map((number) => [number, defaultValue]));
}

function normalizeNumberMap(map: NumberMap): NumberMap {
  const values = [...map.values()];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  return new Map([...map.entries()].map(([key, value]) => [key, (value - min) / range]));
}

function normalizeStringMap(map: StringMap): StringMap {
  const values = [...map.values()];
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  return new Map([...map.entries()].map(([key, value]) => [key, (value - min) / range]));
}

function reverseNumberMap(map: NumberMap): NumberMap {
  return new Map([...map.entries()].map(([key, value]) => [key, 1 - value]));
}

function specialFrequencyMap(draws: Draw[]): NumberMap {
  const scores = createNumberMap();

  for (let index = 0; index < draws.length; index += 1) {
    const weight = 1 / (index + 1);
    const specialNumber = draws[index].specialNumber;
    scores.set(specialNumber, (scores.get(specialNumber) ?? 0) + weight);
  }

  return normalizeNumberMap(scores);
}

function omissionMap(draws: Draw[]): NumberMap {
  const scores = createNumberMap(draws.length + 1);

  for (let index = 0; index < draws.length; index += 1) {
    const specialNumber = draws[index].specialNumber;
    if ((scores.get(specialNumber) ?? draws.length + 1) > index + 1) {
      scores.set(specialNumber, index + 1);
    }
  }

  return normalizeNumberMap(scores);
}

function mainExposureMap(draws: Draw[]): NumberMap {
  const scores = createNumberMap();

  for (let index = 0; index < draws.length; index += 1) {
    const weight = 1 / (index + 1);
    for (const number of decodeDrawNumbers(draws[index])) {
      scores.set(number, (scores.get(number) ?? 0) + weight);
    }
  }

  return normalizeNumberMap(scores);
}

function transitionMap(draws: Draw[]): NumberMap {
  const scores = createNumberMap();
  const currentSpecial = draws[0]?.specialNumber;

  if (!currentSpecial) {
    return scores;
  }

  for (let index = 0; index < draws.length - 1; index += 1) {
    if (draws[index + 1].specialNumber !== currentSpecial) {
      continue;
    }

    const follower = draws[index].specialNumber;
    scores.set(follower, (scores.get(follower) ?? 0) + 1);
  }

  return normalizeNumberMap(scores);
}

function zodiacFrequencyMap(draws: Draw[], year: number): StringMap {
  const scores = new Map<string, number>(ZODIAC_SEQUENCE.map((zodiac) => [zodiac, 0]));

  for (let index = 0; index < draws.length; index += 1) {
    const weight = 1 / (index + 1);
    const zodiac = getZodiacForNumber(draws[index].specialNumber, year);
    scores.set(zodiac, (scores.get(zodiac) ?? 0) + weight);
  }

  return normalizeStringMap(scores);
}

function zodiacOmissionMap(draws: Draw[], year: number): StringMap {
  const scores = new Map<string, number>(ZODIAC_SEQUENCE.map((zodiac) => [zodiac, draws.length + 1]));

  for (let index = 0; index < draws.length; index += 1) {
    const zodiac = getZodiacForNumber(draws[index].specialNumber, year);
    if ((scores.get(zodiac) ?? draws.length + 1) > index + 1) {
      scores.set(zodiac, index + 1);
    }
  }

  return normalizeStringMap(scores);
}

function zodiacTransitionMap(draws: Draw[], year: number): StringMap {
  const scores = new Map<string, number>(ZODIAC_SEQUENCE.map((zodiac) => [zodiac, 0]));
  const currentSpecial = draws[0]?.specialNumber;

  if (!currentSpecial) {
    return scores;
  }

  const currentZodiac = getZodiacForNumber(currentSpecial, year);

  for (let index = 0; index < draws.length - 1; index += 1) {
    const prevZodiac = getZodiacForNumber(draws[index + 1].specialNumber, year);
    if (prevZodiac !== currentZodiac) {
      continue;
    }

    const followerZodiac = getZodiacForNumber(draws[index].specialNumber, year);
    scores.set(followerZodiac, (scores.get(followerZodiac) ?? 0) + 1);
  }

  return normalizeStringMap(scores);
}

function colorGapMap(draws: Draw[]): NumberMap {
  const counts = new Map<string, number>([
    ["红波", 0],
    ["蓝波", 0],
    ["绿波", 0],
  ]);

  for (const draw of draws) {
    const color = getWaveColor(draw.specialNumber);
    counts.set(color, (counts.get(color) ?? 0) + 1);
  }

  const total = [...counts.values()].reduce((sum, value) => sum + value, 0) || 1;
  const colorScore = new Map<string, number>();
  for (const [color, value] of counts.entries()) {
    colorScore.set(color, 1 - value / total);
  }

  const normalized = normalizeStringMap(colorScore);
  return new Map(ALL_NUMBERS.map((number) => [number, normalized.get(getWaveColor(number)) ?? 0]));
}

function zoneGapMap(draws: Draw[]): NumberMap {
  const counts = [0, 0, 0, 0, 0];

  for (const draw of draws) {
    counts[getZoneIndex(draw.specialNumber)] += 1;
  }

  const max = Math.max(...counts, 1);
  return new Map(
    ALL_NUMBERS.map((number) => {
      const zone = getZoneIndex(number);
      return [number, 1 - counts[zone] / max];
    }),
  );
}

function applyRecentPenalty(draws: Draw[], scores: NumberMap): NumberMap {
  const penalized = new Map(scores);

  for (let index = 0; index < Math.min(draws.length, 3); index += 1) {
    const specialNumber = draws[index].specialNumber;
    const penalty = RECENT_SPECIAL_PENALTY.has(index + 1) ? 0.35 : 0.18;
    penalized.set(specialNumber, (penalized.get(specialNumber) ?? 0) - penalty);
  }

  return penalized;
}

function buildReason(parts: Array<[string, number]>): string {
  return parts
    .filter(([, value]) => value > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([label, value]) => `${label} ${value.toFixed(2)}`)
    .join(" · ");
}

function pickTopCandidates(
  scores: NumberMap,
  count: number,
  explain: (number: number, score: number) => string,
): StrategyResult["picks"] {
  return [...scores.entries()]
    .sort((a, b) => b[1] - a[1] || a[0] - b[0])
    .slice(0, count)
    .map(([number, score], index) => ({
      number,
      rank: index + 1,
      score,
      reason: explain(number, score),
    }));
}

export const strategyMeta: Record<StrategyId, { name: string; description: string; limit: number }> = {
  zodiac_special_v1: {
    name: "生肖号码方案",
    description: "按生肖热度、遗漏和转移节奏筛选下一期特别号生肖池",
    limit: 30,
  },
  hot_special_v1: {
    name: "热门号码方案",
    description: "优先选择近期特别号高频、主号带动明显的候选号码",
    limit: 18,
  },
  cold_special_v1: {
    name: "冷门号码方案",
    description: "优先选择长遗漏且具回补条件的特别号候选号码",
    limit: 18,
  },
  knowledge_mix_v1: {
    name: "综合方案",
    description: "融合热度、冷门、生肖、波色、分区和主号联动的综合方案",
    limit: 20,
  },
};

export function generateStrategyResult(strategy: StrategyId, recentDraws: Draw[], issueNo: string): StrategyResult {
  const targetYear = inferYearFromIssue(issueNo, recentDraws[0]?.drawDate.getUTCFullYear());
  const longWindow = recentDraws.slice(0, Math.min(recentDraws.length, 180));
  const mediumWindow = recentDraws.slice(0, Math.min(recentDraws.length, 72));
  const shortWindow = recentDraws.slice(0, Math.min(recentDraws.length, 24));

  const hotLong = specialFrequencyMap(longWindow);
  const hotShort = specialFrequencyMap(shortWindow);
  const cold = omissionMap(longWindow);
  const antiHot = reverseNumberMap(hotLong);
  const mainHot = mainExposureMap(recentDraws.slice(0, Math.min(recentDraws.length, 18)));
  const transition = transitionMap(mediumWindow);
  const colorGap = colorGapMap(shortWindow);
  const zoneGap = zoneGapMap(shortWindow);
  const zodiacHot = zodiacFrequencyMap(longWindow, targetYear);
  const zodiacCold = zodiacOmissionMap(longWindow, targetYear);
  const zodiacTransition = zodiacTransitionMap(mediumWindow, targetYear);

  const hotScores = createNumberMap();
  const coldScores = createNumberMap();
  const mixScores = createNumberMap();

  for (const number of ALL_NUMBERS) {
    const zodiac = getZodiacForNumber(number, targetYear);
    const zodiacMomentum =
      (zodiacHot.get(zodiac) ?? 0) * 0.55 +
      (zodiacCold.get(zodiac) ?? 0) * 0.2 +
      (zodiacTransition.get(zodiac) ?? 0) * 0.25;

    hotScores.set(
      number,
      (hotShort.get(number) ?? 0) * 0.48 +
        (hotLong.get(number) ?? 0) * 0.24 +
        (mainHot.get(number) ?? 0) * 0.14 +
        (transition.get(number) ?? 0) * 0.14,
    );

    coldScores.set(
      number,
      (cold.get(number) ?? 0) * 0.56 +
        (antiHot.get(number) ?? 0) * 0.16 +
        (colorGap.get(number) ?? 0) * 0.1 +
        (zoneGap.get(number) ?? 0) * 0.08 +
        zodiacMomentum * 0.1,
    );

    mixScores.set(
      number,
      (hotScores.get(number) ?? 0) * 0.24 +
        (coldScores.get(number) ?? 0) * 0.2 +
        (mainHot.get(number) ?? 0) * 0.16 +
        zodiacMomentum * 0.16 +
        (transition.get(number) ?? 0) * 0.08 +
        (colorGap.get(number) ?? 0) * 0.08 +
        (zoneGap.get(number) ?? 0) * 0.08,
    );
  }

  const adjustedHot = applyRecentPenalty(recentDraws, hotScores);
  const adjustedCold = applyRecentPenalty(recentDraws, coldScores);
  const adjustedMix = applyRecentPenalty(recentDraws, mixScores);

  if (strategy === "zodiac_special_v1") {
    const zodiacScores = [...ZODIAC_SEQUENCE]
      .map((zodiac) => ({
        zodiac,
        score:
          (zodiacHot.get(zodiac) ?? 0) * 0.45 +
          (zodiacCold.get(zodiac) ?? 0) * 0.25 +
          (zodiacTransition.get(zodiac) ?? 0) * 0.3,
      }))
      .sort((a, b) => b.score - a.score);

    const pickedZodiacs = zodiacScores.slice(0, 6);
    const zodiacScoreMap = new Map(pickedZodiacs.map((item) => [item.zodiac, item.score]));
    const zodiacPool = pickedZodiacs.flatMap(({ zodiac }) =>
      getNumbersForZodiac(zodiac, targetYear).map((number) => {
        const score =
          (zodiacScoreMap.get(zodiac) ?? 0) * 0.58 +
          (adjustedMix.get(number) ?? 0) * 0.24 +
          (mainHot.get(number) ?? 0) * 0.18;
        return [number, score] as const;
      }),
    );

    const limited = new Map(
      zodiacPool
        .sort((a, b) => b[1] - a[1] || a[0] - b[0])
        .slice(0, strategyMeta[strategy].limit),
    );

    return {
      strategy,
      strategyVersion: strategy,
      picks: pickTopCandidates(limited, strategyMeta[strategy].limit, (number, score) => {
        const zodiac = getZodiacForNumber(number, targetYear);
        return buildReason([
          [`${zodiac}热度`, zodiacHot.get(zodiac) ?? 0],
          [`${zodiac}遗漏`, zodiacCold.get(zodiac) ?? 0],
          [`${zodiac}转移`, zodiacTransition.get(zodiac) ?? 0],
          ["综合分", score],
        ]);
      }),
    };
  }

  if (strategy === "hot_special_v1") {
    return {
      strategy,
      strategyVersion: strategy,
      picks: pickTopCandidates(adjustedHot, strategyMeta[strategy].limit, (number, score) =>
        buildReason([
          ["短期热度", hotShort.get(number) ?? 0],
          ["长期热度", hotLong.get(number) ?? 0],
          ["主号带动", mainHot.get(number) ?? 0],
          ["接力转移", transition.get(number) ?? 0],
          ["综合分", score],
        ]),
      ),
    };
  }

  if (strategy === "cold_special_v1") {
    return {
      strategy,
      strategyVersion: strategy,
      picks: pickTopCandidates(adjustedCold, strategyMeta[strategy].limit, (number, score) =>
        buildReason([
          ["遗漏修复", cold.get(number) ?? 0],
          ["冷门纯度", antiHot.get(number) ?? 0],
          ["波色缺口", colorGap.get(number) ?? 0],
          ["分区缺口", zoneGap.get(number) ?? 0],
          ["综合分", score],
        ]),
      ),
    };
  }

  return {
    strategy,
    strategyVersion: strategy,
    picks: pickTopCandidates(adjustedMix, strategyMeta[strategy].limit, (number, score) =>
      buildReason([
        ["热度", hotLong.get(number) ?? 0],
        ["冷门修复", cold.get(number) ?? 0],
        ["主号联动", mainHot.get(number) ?? 0],
        [`${getYearZodiac(targetYear)}年生肖轴`, zodiacHot.get(getZodiacForNumber(number, targetYear)) ?? 0],
        ["综合分", score],
      ]),
    ),
  };
}

export function allStrategies(): StrategyId[] {
  return ["zodiac_special_v1", "hot_special_v1", "cold_special_v1", "knowledge_mix_v1"];
}
