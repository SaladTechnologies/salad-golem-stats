// Dashboard.tsx - Main dashboard component for Stats Salad
// Uses Material-UI, Chart.tsx, react-globe.gl, and custom chart components

import { useState, useEffect, useMemo, ReactNode } from 'react';
import type { Theme, SxProps } from '@mui/material';
import {
  Container,
  Typography,
  Box,
  Paper,
  Grid,
  ThemeProvider,
  createTheme,
  CssBaseline,
  GlobalStyles,
  Switch,
  FormControlLabel,
  PaperProps,
} from '@mui/material';
import { TrendChart, StackedChart, TrendDataPoint, ChartData } from './charts';
import MetricsBar from './MetricsBar';
import TransactionsTable from './TransactionsTable';
import GlobeComponent, { GeoData } from './Globe';

// API response types
interface TimeSeriesPoint {
  timestamp: string;
  active_nodes?: number;
  total_fees?: number;
  compute_hours?: number;
  ram_hours?: number;
  core_hours?: number;
  gpu_hours?: number;
}

interface PlansMetricsTotals {
  active_nodes?: number;
  total_fees?: number;
  compute_hours?: number;
}

interface PlansMetricsData {
  totals?: PlansMetricsTotals;
  time_series?: TimeSeriesPoint[];
  active_nodes_by_gpu_model_ts?: ChartData;
  active_nodes_by_vram_ts?: ChartData;
  gpu_hours_by_model_ts?: ChartData;
  gpu_hours_by_vram_ts?: ChartData;
}

interface Transaction {
  tx_hash?: string;
  block_number?: number;
  block_timestamp?: string;
  from_address?: string;
  to_address?: string;
  value_glm?: number;
  tx_type?: string;
}

// Hook for fetching plan metrics from the new /metrics/plans endpoint
function usePlansMetrics(period = '7d') {
  const [data, setData] = useState<PlansMetricsData | null | undefined>(undefined);
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;
    setIsLoading(true);
    fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/plans?period=${period}`)
      .then((res) =>
        res.ok
          ? (res.json() as Promise<PlansMetricsData>)
          : Promise.reject('Failed to fetch plans metrics'),
      )
      .then((result) => {
        if (!cancelled) {
          setData(result);
          setIsLoading(false);
        }
      })
      .catch((err) => {
        console.error('Error loading plans metrics:', err);
        if (!cancelled) {
          setData(null);
          setIsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [period]);

  return { data, isLoading };
}

// Transform time_series to TrendChart format: [{x: timestamp, y: value}]
function transformTimeSeries(
  timeSeries: TimeSeriesPoint[] | undefined,
  metric: keyof TimeSeriesPoint,
): TrendDataPoint[] {
  if (!timeSeries || !Array.isArray(timeSeries)) return [];
  return timeSeries.map((point) => ({
    x: new Date(point.timestamp).getTime(),
    y: (point[metric] as number) || 0,
  }));
}

// Custom color palette
const saladPalette = {
  green: 'rgb(83,166,38)', // Main accent green
  darkGreen: 'rgb(31,79,34)', // Darker green
  lime: 'rgb(178,213,48)', // Lime accent
  midGreen: 'rgb(120,200,60)', // Midway green for globe bars in dark mode
  lightGreen: 'rgb(219,243,193)', // Light green background
  navy: 'rgb(10,33,51)', // Deep navy for backgrounds
};

interface StyledPaperProps extends Omit<PaperProps, 'sx'> {
  children: ReactNode;
  sx?: SxProps<Theme>;
}

// StyledPaper: reusable Paper with common dashboard styles
function StyledPaper({ children, sx, ...props }: StyledPaperProps) {
  return (
    <Paper
      elevation={2}
      sx={[
        (theme: Theme) => ({
          pt: 1,
          mb: 4,
          borderRadius: 2,
          px: 4,
          pb: 4,
          bgcolor: theme.palette.background.paper,
          border: `1px solid ${theme.palette.mode === 'dark' ? '#333' : '#e0e0e0'}`,
          backgroundImage: 'none',
          maxWidth: '96vw',
          mx: 'auto',
        }),
        ...(Array.isArray(sx) ? sx : sx ? [sx] : []),
      ]}
      className="w-block"
      {...props}
    >
      {children}
    </Paper>
  );
}

interface StyledHeadingProps {
  children: ReactNode;
  mt?: number;
}

// StyledHeading: reusable heading with border line
function StyledHeading({ children, mt = 2 }: StyledHeadingProps) {
  return (
    <>
      <Typography
        variant="h5"
        sx={(theme: Theme) => ({
          mt,
          mb: 0.5,
          color: theme.palette.primary.main,
          fontSize: '1.75rem',
        })}
        className="w-block"
      >
        {children}
      </Typography>
      <Box
        sx={(theme: Theme) => ({
          width: '90%',
          borderBottom: `2px solid ${theme.palette.mode === 'dark' ? 'rgba(219,243,193,0.3)' : 'rgb(219,243,193)'}`,
          mb: 2,
        })}
        className="w-clearfix"
      />
    </>
  );
}

// Material-UI theme functions for light and dark modes
const createAppTheme = (mode: 'light' | 'dark') =>
  createTheme({
    palette: {
      mode,
      primary: {
        main: mode === 'dark' ? saladPalette.lime : saladPalette.green,
        dark: saladPalette.darkGreen,
        light: saladPalette.lime,
        contrastText: '#fff',
      },
      secondary: {
        main: saladPalette.lime,
        dark: saladPalette.green,
        light: saladPalette.lightGreen,
        contrastText: mode === 'dark' ? '#fff' : saladPalette.navy,
      },
      background: {
        default: mode === 'dark' ? '#121212' : '#fff',
        paper: mode === 'dark' ? '#121212' : '#fff',
      },
      text: {
        primary: mode === 'dark' ? 'rgba(255, 255, 255, 0.85)' : saladPalette.navy,
        secondary: mode === 'dark' ? '#b0b0b0' : saladPalette.darkGreen,
      },
    },
    typography: {
      fontFamily:
        'ui-sans-serif, system-ui, sans-serif, Apple Color Emoji, Segoe UI Emoji, Segoe UI Symbol, Noto Color Emoji',
      fontSize: 14,
      h1: {
        fontWeight: 700,
        fontSize: 38,
        lineHeight: '44px',
        marginTop: 20,
        marginBottom: 10,
        color: mode === 'dark' ? saladPalette.lime : saladPalette.green,
      },
      h2: {
        fontWeight: 700,
        fontSize: 32,
        lineHeight: '36px',
        marginTop: 20,
        marginBottom: 10,
        color: mode === 'dark' ? saladPalette.lime : saladPalette.green,
      },
      h3: {
        fontWeight: 700,
        fontSize: 24,
        lineHeight: '30px',
        marginTop: 20,
        marginBottom: 10,
        color: mode === 'dark' ? saladPalette.lime : saladPalette.green,
      },
      h4: {
        fontWeight: 700,
        fontSize: 18,
        lineHeight: '24px',
        marginTop: 10,
        marginBottom: 10,
        color: mode === 'dark' ? saladPalette.lime : saladPalette.green,
      },
      h5: {
        fontWeight: 700,
        fontSize: 24,
        lineHeight: '30px',
        marginTop: 16,
        marginBottom: 12,
        color: mode === 'dark' ? saladPalette.lime : saladPalette.green,
      },
      h6: {
        fontWeight: 700,
        fontSize: 18,
        lineHeight: '24px',
        marginTop: 10,
        marginBottom: 10,
        color: mode === 'dark' ? saladPalette.lime : saladPalette.green,
      },
      body1: { fontSize: 14, lineHeight: '20px', color: '#333' },
    },
    breakpoints: {
      values: {
        xs: 0,
        sm: 600,
        md: 800,
        lg: 1200,
        xl: 1536,
      },
    },
  });

type TimeWindow = '6h' | '24h' | '7d' | '30d' | '90d' | 'total';

export default function Dashboard() {
  // Initialize theme from localStorage or system preference

  const getInitialTheme = (): 'light' | 'dark' => {
    const saved = localStorage.getItem('theme');
    if (saved === 'light' || saved === 'dark') return saved;

    // Fallback to system preference
    if (typeof window !== 'undefined' && window.matchMedia) {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    return 'light';
  };

  // Global time window state for all charts - now using plan periods, with persistence
  const [globalTimeWindow, setGlobalTimeWindow] = useState<TimeWindow>(() => {
    if (typeof window !== 'undefined') {
      const saved = localStorage.getItem('timeWindow');
      if (saved && ['6h', '24h', '7d', '30d', '90d', 'total'].includes(saved)) {
        return saved as TimeWindow;
      }
    }
    return '7d';
  });
  // Track which time window button is loading
  const [loadingTimeWindow, setLoadingTimeWindow] = useState<TimeWindow | null>(null);
  // Theme mode state with persistence
  const [themeMode, setThemeMode] = useState<'light' | 'dark'>(getInitialTheme);

  // Save time window to localStorage when it changes
  useEffect(() => {
    localStorage.setItem('timeWindow', globalTimeWindow);
  }, [globalTimeWindow]);

  // Save theme to localStorage when it changes
  useEffect(() => {
    localStorage.setItem('theme', themeMode);
  }, [themeMode]);

  // Memoize theme so it only changes when themeMode changes
  const theme = useMemo(() => createAppTheme(themeMode), [themeMode]);

  // Fetch plan metrics from the new endpoint
  const { data: plansData, isLoading } = usePlansMetrics(globalTimeWindow);

  // Clear loading state when data arrives
  useEffect(() => {
    if (!isLoading) {
      setLoadingTimeWindow(null);
    }
  }, [isLoading]);
  // State for geo node data
  const [geoData, setGeoData] = useState<GeoData | null | undefined>(undefined);
  // State for transactions data
  const [transactions, setTransactions] = useState<Transaction[] | null | undefined>(undefined);

  // Fetch transactions on mount
  useEffect(() => {
    let cancelled = false;
    fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/transactions?limit=10`)
      .then((res) =>
        res.ok
          ? (res.json() as Promise<{ transactions?: Transaction[] }>)
          : Promise.reject('Failed to fetch transactions'),
      )
      .then((data) => {
        if (!cancelled) setTransactions(data.transactions || []);
      })
      .catch((err) => {
        console.error('Error loading transactions:', err);
        if (!cancelled) setTransactions(null);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  // Load node count data from API endpoint only once on mount
  useEffect(() => {
    let cancelled = false;
    fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/geo_counts`)
      .then((res) =>
        res.ok ? (res.json() as Promise<GeoData>) : Promise.reject('Failed to fetch geo data'),
      )
      .then((data) => {
        if (!cancelled) setGeoData(data);
      })
      .catch((err) => {
        console.error('Error loading geoData:', err);
        if (!cancelled) setGeoData(null);
      });
    return () => {
      cancelled = true;
    };
  }, []); // Only run on mount, not on time window change

  const timeWindowOptions: { key: TimeWindow; label: string }[] = [
    { key: '6h', label: '6h' },
    { key: '24h', label: '24h' },
    { key: '7d', label: '7d' },
    { key: '30d', label: '30d' },
    { key: '90d', label: '90d' },
    { key: 'total', label: 'All' },
  ];

  const periodLabels: Record<TimeWindow, string> = {
    '6h': 'last 6 hours',
    '24h': 'last 24 hours',
    '7d': 'last 7 days',
    '30d': 'last 30 days',
    '90d': 'last 90 days',
    total: 'all time',
  };

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <GlobalStyles
        styles={{
          '@font-face': {
            fontFamily: 'webflow-icons',
            src: "url(data:application/x-font-ttf;charset=utf-8;base64,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) format('truetype')",
            fontWeight: 400,
            fontStyle: 'normal',
          },
          '*': {
            boxSizing: 'border-box',
          },
          body: {
            margin: 0,
            minHeight: '100%',
            backgroundColor: '#fff',
            fontFamily: 'Arial, sans-serif',
            fontSize: '14px',
            lineHeight: '20px',
            color: '#333',
          },
          img: {
            maxWidth: '100%',
            verticalAlign: 'middle',
            display: 'inline-block',
          },
          h1: {
            fontWeight: 700,
            fontSize: '38px',
            lineHeight: '44px',
            marginTop: '20px',
            marginBottom: '10px',
          },
          h2: {
            fontWeight: 700,
            fontSize: '32px',
            lineHeight: '36px',
            marginTop: '20px',
            marginBottom: '10px',
          },
          h3: {
            fontWeight: 700,
            fontSize: '24px',
            lineHeight: '30px',
            marginTop: '20px',
            marginBottom: '10px',
          },
          h4: {
            fontWeight: 700,
            fontSize: '18px',
            lineHeight: '24px',
            marginTop: '10px',
            marginBottom: '10px',
          },
          h5: {
            fontWeight: 700,
            fontSize: '14px',
            lineHeight: '20px',
            marginTop: '10px',
            marginBottom: '10px',
          },
          h6: {
            fontWeight: 700,
            fontSize: '12px',
            lineHeight: '18px',
            marginTop: '10px',
            marginBottom: '10px',
          },
          '.w-block': { display: 'block' },
          '.w-inline-block': { display: 'inline-block', maxWidth: '100%' },
          '.w-clearfix:before, .w-clearfix:after': {
            content: '" "',
            display: 'table',
          },
          '.w-clearfix:after': { clear: 'both' },
          '.w-hidden': { display: 'none' },
          '.w-button': {
            display: 'inline-block',
            padding: '9px 15px',
            backgroundColor: '#3898ec',
            color: '#fff',
            border: 0,
            lineHeight: 'inherit',
            textDecoration: 'none',
            cursor: 'pointer',
            borderRadius: 0,
          },
        }}
      />
      {/* Top nav bar with logo - always full width, fixed */}
      <Box
        sx={{
          width: '100vw',
          minWidth: '100vw',
          maxWidth: '100vw',
          position: 'fixed',
          top: 0,
          left: 0,
          height: 64,
          bgcolor:
            (theme.palette as { salad?: { navy?: string } }).salad?.navy || saladPalette.navy,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          px: 3,
          borderBottom: '4px solid',
          borderColor:
            (theme.palette as { salad?: { navy?: string } }).salad?.navy || saladPalette.navy,
          zIndex: 1200,
          boxSizing: 'border-box',
        }}
        className="w-block"
      >
        <a
          href="https://portal.salad.com"
          target="_blank"
          rel="noopener noreferrer"
          style={{ display: 'flex', alignItems: 'center', height: '100%' }}
        >
          <img
            src="/salad-logo-light-4x-DpLeJfaD.png"
            alt="Salad Logo"
            style={{ height: 46.15, width: 'auto', display: 'block' }}
          />
        </a>
        <Box sx={{ display: 'flex', gap: 1, mr: 3, alignItems: 'center' }}>
          {timeWindowOptions.map((opt) => (
            <button
              key={opt.key}
              style={{
                padding: '4px 12px',
                fontSize: '0.9rem',
                background:
                  loadingTimeWindow === opt.key
                    ? '#666'
                    : globalTimeWindow === opt.key
                      ? saladPalette.green
                      : 'transparent',
                color: '#fff',
                border: '1px solid #999',
                borderRadius: '4px',
                cursor: 'pointer',
                minWidth: '32px',
                outline: 'none',
              }}
              onMouseEnter={(e) => {
                if (loadingTimeWindow !== opt.key && globalTimeWindow !== opt.key) {
                  e.currentTarget.style.background = 'rgb(83,166,38)';
                  e.currentTarget.style.color = '#fff';
                }
              }}
              onMouseLeave={(e) => {
                if (loadingTimeWindow !== opt.key && globalTimeWindow !== opt.key) {
                  e.currentTarget.style.background = 'transparent';
                  e.currentTarget.style.color = '#fff';
                }
              }}
              onClick={() => {
                setLoadingTimeWindow(opt.key);
                setGlobalTimeWindow(opt.key);
              }}
            >
              {opt.label}
            </button>
          ))}
          <Box sx={{ ml: 2, borderLeft: '1px solid #666', pl: 2 }}>
            <FormControlLabel
              control={
                <Switch
                  checked={themeMode === 'dark'}
                  onChange={(e) => setThemeMode(e.target.checked ? 'dark' : 'light')}
                  size="small"
                  sx={{
                    '& .MuiSwitch-switchBase.Mui-checked': {
                      color: saladPalette.green,
                    },
                    '& .MuiSwitch-switchBase.Mui-checked + .MuiSwitch-track': {
                      backgroundColor: saladPalette.green,
                    },
                  }}
                />
              }
              label={themeMode === 'dark' ? '🌙' : '☀'}
              labelPlacement="start"
              sx={{
                color: '#fff',
                m: 0,
                '& .MuiFormControlLabel-label': {
                  fontSize: '1.1rem',
                  mr: 1,
                },
              }}
            />
          </Box>
        </Box>
      </Box>
      <Box
        sx={{
          bgcolor: theme.palette.background.default,
          minHeight: '100vh',
          pt: '80px',
          overflowX: 'auto',
          width: '100vw',
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
        }}
      >
        <Container
          maxWidth="xl"
          sx={{
            bgcolor: 'transparent',
            px: { xs: 1, sm: 2, md: 6 },
            mx: 'auto',
            display: 'block',
          }}
        >
          {/* MetricsBar: summary metrics above network activity */}
          {plansData === undefined ? (
            <Typography variant="body2" color="textSecondary">
              Loading metrics...
            </Typography>
          ) : plansData === null ? (
            <Typography variant="body2" color="error">
              Failed to load metrics.
            </Typography>
          ) : (
            (() => {
              const periodLabel = periodLabels[globalTimeWindow] || globalTimeWindow;
              const totals = plansData.totals || {};
              return (
                <MetricsBar
                  key={globalTimeWindow + '-' + JSON.stringify(totals)}
                  metrics={[
                    {
                      value: totals.active_nodes ?? 0,
                      unit: '',
                      label: `Active nodes (${periodLabel})`,
                    },
                    {
                      value: totals.total_fees ?? 0,
                      unit: '$',
                      label: `Fees paid (${periodLabel})`,
                    },
                    {
                      value: totals.compute_hours ?? 0,
                      unit: 'hrs',
                      label: `Hours of compute (${periodLabel})`,
                    },
                  ]}
                />
              );
            })()
          )}
          {/* Dashboard Title and Intro */}
          <Grid container spacing={4} sx={{ alignItems: 'stretch' }} direction="row">
            <Grid
              size={{ xs: 12, md: 6 }}
              sx={{ display: 'flex', flexDirection: 'column', maxWidth: 650 }}
            >
              <Box sx={{ mb: 1, mt: 4 }} className="w-block">
                <Typography
                  variant="h2"
                  component="h1"
                  sx={{
                    color: theme.palette.text.primary,
                    textAlign: 'left',
                    fontWeight: 700,
                    mb: 0,
                    mt: 0,
                  }}
                  className="w-block"
                >
                  SaladCloud Network Statistics
                </Typography>
              </Box>
              <Paper
                elevation={0}
                sx={{
                  p: 3,
                  bgcolor: theme.palette.background.paper,
                  borderRadius: 3,
                  border: 'none',
                  boxShadow: 'none',
                  width: '100%',
                }}
              >
                <Typography
                  variant="body1"
                  sx={{
                    color: theme.palette.text.primary,
                    textAlign: 'justify',
                    fontSize: '1.0rem',
                    letterSpacing: 0.01,
                    lineHeight: 1.7,
                  }}
                >
                  This page provides summary statistics from SaladCloud's testing on the Golem
                  Network. These tests support a broader initiative to evaluate the use of the GLM
                  token for facilitating compute transactions across SaladCloud.
                  <br />
                  <br />
                  SaladCloud is a Web2 distributed cloud computing platform enabling customers to
                  run workloads including text-to-image, text-to-video, molecular simulations, and
                  zero-knowledge proofs. SaladCloud nodes are worldwide, as seen in the distribution
                  of daily active SaladCloud nodes in the globe to the right in the full network.
                  The data presented represents a subset of participating customers (requestors) and
                  network nodes (providers), and includes test compute transactions executed
                  on-chain using GLM.
                </Typography>
              </Paper>
            </Grid>
            <Grid
              size={{ xs: 12, md: 6 }}
              sx={{ display: 'flex', flexDirection: 'column', justifyContent: 'center' }}
            >
              {/* Distribution Section*/}
              <Box
                sx={{
                  width: '100%',
                  bgcolor: theme.palette.background.paper,
                  display: 'flex',
                  flexDirection: 'column',
                  alignItems: 'center',
                  mb: 0,
                }}
                className="w-block"
              >
                {geoData === undefined ? (
                  <Typography variant="body2" color="textSecondary">
                    Loading globe data...
                  </Typography>
                ) : geoData === null ? (
                  <Typography variant="body2" color="error">
                    Failed to load globe data.
                  </Typography>
                ) : (
                  <GlobeComponent theme={theme} themeMode={themeMode} geoData={geoData} />
                )}
              </Box>
            </Grid>
          </Grid>

          <StyledPaper sx={{ pb: 0 }}>
            {/* Transactions Table */}
            <Box sx={{ width: '100%' }}>
              <StyledHeading>Network Transactions</StyledHeading>
              {transactions === undefined ? (
                <Typography variant="body2" color="textSecondary">
                  Loading transactions...
                </Typography>
              ) : transactions === null ? (
                <Typography variant="body2" color="error">
                  Failed to load transactions.
                </Typography>
              ) : (
                <TransactionsTable />
              )}
            </Box>
          </StyledPaper>
          <StyledPaper>
            <StyledHeading>Network Activity</StyledHeading>
            <Grid container spacing={3} justifyContent="center">
              {plansData ? (
                <>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="unique_node_count"
                      title="Active Unique Nodes"
                      description="Number of SaladCloud providers that ran workloads."
                      trendWindow={globalTimeWindow}
                      trendData={transformTimeSeries(plansData.time_series, 'active_nodes')}
                      unit=""
                      unitType="front"
                      isLoading={isLoading}
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-invoice-amount"
                      title="Fees Paid ($)"
                      description="Fees paid by customers for workloads running on SaladCloud."
                      trendWindow={globalTimeWindow}
                      trendData={transformTimeSeries(plansData.time_series, 'total_fees')}
                      unit="$"
                      unitType="front"
                      isLoading={isLoading}
                    />
                  </Grid>

                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-time-hours"
                      title="Compute Time (hr)"
                      description="Time customer workloads ran on SaladCloud."
                      trendWindow={globalTimeWindow}
                      trendData={transformTimeSeries(plansData.time_series, 'compute_hours')}
                      unit="hours"
                      unitType="below"
                      isLoading={isLoading}
                    />
                  </Grid>
                </>
              ) : (
                <Typography variant="body2" color="textSecondary">
                  Loading...
                </Typography>
              )}
            </Grid>
          </StyledPaper>
          <StyledPaper>
            {/* Compute Resources Usage Section */}
            <StyledHeading>Resource Usage</StyledHeading>
            <Grid container spacing={3} justifyContent="center">
              {plansData ? (
                <>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-ram-hours"
                      title="Memory (GB-hr)"
                      description="Aggregated RAM usage across all workloads and provider nodes."
                      trendWindow={globalTimeWindow}
                      trendData={transformTimeSeries(plansData.time_series, 'ram_hours')}
                      unit="GB-hr"
                      unitType="below"
                      isLoading={isLoading}
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-cpu-hours"
                      title="vCPUs (vCPU-hr)"
                      description="Aggregated vCPU usage across all workloads and provider nodes."
                      trendWindow={globalTimeWindow}
                      trendData={transformTimeSeries(plansData.time_series, 'core_hours')}
                      unit="CPU-hr"
                      unitType="below"
                      isLoading={isLoading}
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <StackedChart
                      id="gpuStackedChart"
                      title="GPUs Used by Model"
                      description="Provider nodes running GPU workloads, by model."
                      trendWindow={globalTimeWindow}
                      chartData={plansData.active_nodes_by_gpu_model_ts}
                      labels={plansData.active_nodes_by_gpu_model_ts?.labels || []}
                      unit="nodes"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <StackedChart
                      id="gpuStackedChartVram"
                      title="GPUs Used by VRAM"
                      description="Provider nodes running GPU workloads, by VRAM."
                      trendWindow={globalTimeWindow}
                      chartData={plansData.active_nodes_by_vram_ts}
                      labels={plansData.active_nodes_by_vram_ts?.labels || []}
                      unit="nodes"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <StackedChart
                      id="gpuStackedChartTime"
                      title="GPUs Time by Model (hr)"
                      description="Time GPU customer workloads were running on SaladCloud, by GPU."
                      trendWindow={globalTimeWindow}
                      chartData={plansData.gpu_hours_by_model_ts}
                      labels={plansData.gpu_hours_by_model_ts?.labels || []}
                      unit="hours"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <StackedChart
                      id="gpuStackedChartVramTime"
                      title="GPUs Time by VRAM (hr)"
                      description="Time GPU customer workloads were running on SaladCloud, by VRAM."
                      trendWindow={globalTimeWindow}
                      chartData={plansData.gpu_hours_by_vram_ts}
                      labels={plansData.gpu_hours_by_vram_ts?.labels || []}
                      unit="hours"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-gpu-hours"
                      title="GPU Time (hr)"
                      description="Total GPU compute hours across all workloads."
                      trendWindow={globalTimeWindow}
                      trendData={transformTimeSeries(plansData.time_series, 'gpu_hours')}
                      unit="GPU-hrs"
                      unitType="below"
                      isLoading={isLoading}
                    />
                  </Grid>
                </>
              ) : (
                <Typography variant="body2" color="textSecondary">
                  Loading...
                </Typography>
              )}
            </Grid>
          </StyledPaper>
        </Container>
      </Box>
    </ThemeProvider>
  );
}
