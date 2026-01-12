import 'dotenv/config';

export const config = {
  dataDirectory: process.env.DATA_DIRECTORY || './data',

  mixpanel: {
    apiKey: process.env.MIXPANEL_API_KEY || '',
    jqlUrl: process.env.MIXPANEL_JQL_URL || 'https://mixpanel.com/api/query/jql',
  },

  postgres: {
    host: process.env.POSTGRES_HOST || 'localhost',
    port: parseInt(process.env.POSTGRES_PORT || '5432', 10),
    database: process.env.POSTGRES_DB || 'statsdb',
    user: process.env.POSTGRES_USER || 'devuser',
    password: process.env.POSTGRES_PASSWORD || 'devpass',
    ssl: process.env.POSTGRES_SSL === 'true',
  },

  // Minimum duration in ms (10 minutes)
  minimumDuration: parseInt(process.env.MINIMUM_DURATION || '600000', 10),

  // Import interval in ms (6 hours)
  importInterval: parseInt(process.env.IMPORT_INTERVAL || '21600000', 10),

  // Optional org name filter (comma-separated list)
  orgNameFilter: process.env.ORG_NAME_FILTER
    ? process.env.ORG_NAME_FILTER.split(',').map((s) => s.trim()).filter(Boolean)
    : [],
} as const;
