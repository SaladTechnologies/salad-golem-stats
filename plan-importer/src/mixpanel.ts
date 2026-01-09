import fs from 'fs/promises';
import path from 'path';
import { DateTime } from 'luxon';
import { config } from './config.js';
import { logger } from './logger.js';

interface DateRange {
  filename: string;
  start: string;
  end: string;
}

/**
 * Get the previous n day ranges based on Mountain Time.
 */
function getPreviousDayRanges(n: number, baseDate?: Date): DateRange[] {
  const base = baseDate
    ? DateTime.fromJSDate(baseDate, { zone: 'America/Denver' })
    : DateTime.now().setZone('America/Denver');

  const pad = (x: number) => x.toString().padStart(2, '0');
  const formatCompact = (d: DateTime) => `${d.year}${pad(d.month)}${pad(d.day)}`;
  const formatDashed = (d: DateTime) => `${d.year}-${pad(d.month)}-${pad(d.day)}`;

  logger.info(`Base date for JQL fetch: ${base.toISO()}`);

  const result: DateRange[] = [];
  for (let i = n + 1; i >= 2; i--) {
    const start = base.minus({ days: i });
    const end = base.minus({ days: i - 1 });
    result.push({
      filename: `${formatCompact(start)}-${formatCompact(end)}.json`,
      start: formatDashed(start),
      end: formatDashed(end),
    });
  }
  return result;
}

/**
 * Fetch data from MixPanel JQL API for the previous two days.
 */
export async function fetchMixpanelJql(): Promise<void> {
  const ranges = getPreviousDayRanges(2);

  const importedDir = path.join(config.dataDirectory, 'imported');
  const pendingDir = path.join(config.dataDirectory, 'pending');

  await fs.mkdir(importedDir, { recursive: true });
  await fs.mkdir(pendingDir, { recursive: true });

  for (const { filename, start, end } of ranges) {
    // Skip if already imported
    const importedPath = path.join(importedDir, filename);
    try {
      await fs.access(importedPath);
      logger.info(`File ${filename} already exists in imported folder. Skipping.`);
      continue;
    } catch {
      logger.info(`File ${filename} does not exist in imported folder. Proceeding to fetch.`);
    }

    logger.info(`Fetching data for ${filename} from MixPanel JQL API...`);

    const jqlQuery = await fs.readFile(
      path.join(import.meta.dirname, '..', 'jql', 'earnings-query.jql'),
      'utf-8'
    );

    const encodedParams = new URLSearchParams();
    encodedParams.set('script', jqlQuery);
    encodedParams.set(
      'params',
      JSON.stringify({
        from_date: end,
        to_date: end,
        event_selectors: [
          { event: 'Workload Earning', selector: 'properties["InvoiceAmount"] > 0' },
        ],
      })
    );

    const response = await fetch(config.mixpanel.jqlUrl, {
      method: 'POST',
      headers: {
        accept: 'application/json',
        'content-type': 'application/x-www-form-urlencoded',
        authorization: 'Basic ' + Buffer.from(`${config.mixpanel.apiKey}:`).toString('base64'),
      },
      body: encodedParams,
    });

    if (!response.ok) {
      logger.error(`MixPanel API error for ${filename}: ${response.status} ${response.statusText}`);
      continue;
    }

    const data = await response.json();

    const pendingPath = path.join(pendingDir, filename);
    await fs.writeFile(pendingPath, JSON.stringify(data, null, 2), 'utf-8');
    logger.info(`Saved ${filename} to pending folder.`);
  }
}
