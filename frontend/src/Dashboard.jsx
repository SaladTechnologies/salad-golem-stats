// Dashboard.jsx - Main dashboard component for Stats Salad
// Uses Material-UI, Chart.jsx, react-globe.gl, and custom chart components

import React, { useState, useRef, useEffect } from 'react';
import {
  Container,
  Typography,
  Box,
  Paper,
  Grid,
  ThemeProvider,
  createTheme,
  Tabs,
  Tab,
  CssBaseline,
  GlobalStyles,
  Switch,
  FormControlLabel,
} from '@mui/material';
import { TrendChart, StackedChart } from './charts.jsx';
import TransactionsTable from './TransactionsTable.jsx';
import GlobeComponent from './Globe.jsx';

// Simple CSV parser for node_count_by_city.csv
function parseCityCSV(text) {
  const lines = text.trim().split(/\r?\n/);
  const header = lines[0].split(',');
  return lines
    .slice(1)
    .map((line) => {
      const cols = line.split(',');
      return {
        city: cols[0],
        count: Number(cols[1]),
        lat: cols[2] ? Number(cols[2]) : undefined,
        lon: cols[3] ? Number(cols[3]) : undefined,
      };
    })
    .filter((row) => row.lat && row.lon && row.count > 0);
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

// Material-UI theme functions for light and dark modes
const createAppTheme = (mode) =>
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
      salad: saladPalette, // custom for direct access
    },
    typography: {
      fontFamily:
        'ui-sans-serif, system-ui, sans-serif, Apple Color Emoji, Segoe UI Emoji, Segoe UI Symbol, Noto Color Emoji',
      fontSize: 14,
      lineHeight: '20px',
      h1: { fontWeight: 700, fontSize: 38, lineHeight: '44px', marginTop: 20, marginBottom: 10, color: mode === 'dark' ? saladPalette.lime : saladPalette.green },
      h2: { fontWeight: 700, fontSize: 32, lineHeight: '36px', marginTop: 20, marginBottom: 10, color: mode === 'dark' ? saladPalette.lime : saladPalette.green },
      h3: { fontWeight: 700, fontSize: 24, lineHeight: '30px', marginTop: 20, marginBottom: 10, color: mode === 'dark' ? saladPalette.lime : saladPalette.green },
      h4: { fontWeight: 700, fontSize: 18, lineHeight: '24px', marginTop: 10, marginBottom: 10, color: mode === 'dark' ? saladPalette.lime : saladPalette.green },
      h5: { fontWeight: 700, fontSize: 24, lineHeight: '30px', marginTop: 16, marginBottom: 12, color: mode === 'dark' ? saladPalette.lime : saladPalette.green },
      h6: { fontWeight: 700, fontSize: 18, lineHeight: '24px', marginTop: 10, marginBottom: 10, color: mode === 'dark' ? saladPalette.lime : saladPalette.green },
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

export default function Dashboard() {
  // Initialize theme from localStorage or system preference
  
  
    const getInitialTheme = () => {
    const saved = localStorage.getItem('theme');
    if (saved) return saved;

    // Fallback to system preference
    if (typeof window !== 'undefined' && window.matchMedia) {
      return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
    }
    return 'light';
  };

  // Global time window state for all charts
  const [globalTimeWindow, setGlobalTimeWindow] = useState('month');
  // Loading state for time selector
  const [isLoadingTimeData, setIsLoadingTimeData] = useState(false);
  // Theme mode state with persistence
  const [themeMode, setThemeMode] = useState(getInitialTheme);

  // Save theme to localStorage when it changes
  useEffect(() => {
    localStorage.setItem('theme', themeMode);
  }, [themeMode]);

  // Create theme based on current mode
  const theme = createAppTheme(themeMode);

  // Helper to fetch stats summary for the bottom section
  function useStatsSummary(period = 'month', gpu = 'all') {
    const [stats, setStats] = useState(null);
    useEffect(() => {
      fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/stats?period=${period}&gpu=${gpu}`)
        .then((res) => (res.ok ? res.json() : Promise.reject('Failed to fetch stats')))
        .then((data) => {
          setStats(data);
          setIsLoadingTimeData(false);
        })
        .catch((err) => {
          console.error('Error loading stats summary:', err);
          setStats(null);
          setIsLoadingTimeData(false);
        });
    }, [period, gpu]);
    return stats;
  }

  // Transform gpu_unique_node_count from statsSummary into chartData for StackedChart
  function getGpuModelStackedDataFromStats(statsSummary) {
    if (!statsSummary || !statsSummary.gpu_unique_node_count) return null;
    const data = statsSummary.gpu_unique_node_count;
    // data: [ { group, values: [ {ts, value}, ... ] }, ... ]
    // Collect all unique timestamps for x-axis
    const allTimestamps = Array.from(
      new Set(
        data.flatMap((groupObj) => groupObj.values.map((v) => v.ts))
      )
    ).sort();
    // GPU group names as labels (for legend)
    const groupLabels = data.map((groupObj) => groupObj.group);

    // Calculate total values for each GPU group to sort by highest to lowest
    const groupTotals = data.map((groupObj) => {
      const total = groupObj.values.reduce((sum, v) => sum + (v.value || 0), 0);
      return { groupObj, total };
    }).sort((a, b) => b.total - a.total);

    // Use the same 6-color green gradient as the legend (lightest to darkest for highest to lowest)
    const legendColors = ['#b2d530', '#9acc35', '#7bb82e', '#53a626', '#3d6b28', '#1f4f22'];

    const datasets = groupTotals.map((item, i) => ({
      label: item.groupObj.group,
      data: allTimestamps.map((ts) => {
        const found = item.groupObj.values.find((v) => v.ts === ts);
        return found ? found.value : 0;
      }),
      backgroundColor: legendColors[i % legendColors.length],
      borderColor: '#fff',
      borderWidth: 1,
      fill: true,
    }));

    // Return both x-axis (timestamps) and legend labels (gpu groups) with sorted order
    return { labels: allTimestamps, datasets, groupLabels: groupTotals.map(item => item.groupObj.group) };
  }

  const statsSummary = useStatsSummary(globalTimeWindow, 'all');
  // State for city node data
  const [cityData, setCityData] = useState([]);

  // State for transactions data
  const [transactions, setTransactions] = useState([]);

  // Fetch transactions on mount
  useEffect(() => {
    fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/transactions?limit=10`)
      .then((res) => (res.ok ? res.json() : Promise.reject('Failed to fetch transactions')))
      .then((data) => setTransactions(data.transactions || []))
      .catch((err) => {
        console.error('Error loading transactions:', err);
        setTransactions([]);
      });
  }, []);

  // Load city node count data from API endpoint on mount
  useEffect(() => {
    fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/city_counts`)
      .then((res) => (res.ok ? res.json() : Promise.reject('Failed to fetch city data')))
      .then((data) => {
        setCityData(data);
      })
      .catch((err) => {
        console.error('Error loading cityData:', err);
      });
  }, []);

  let gpuModelStackedData = getGpuModelStackedDataFromStats(statsSummary);
  if (!gpuModelStackedData || !gpuModelStackedData.labels) {
    gpuModelStackedData = { labels: [], datasets: [] };
  }

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
          bgcolor: theme.palette.salad.navy,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          px: 3,
          borderBottom: '4px solid',
          borderColor: theme.palette.salad.navy,
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
          {[
            { key: 'day', label: '1d' },
            { key: 'week', label: '7d' },
            { key: 'month', label: '1m' },
          ].map((opt) => (
            <button
              key={opt.key}
              disabled={isLoadingTimeData}
              style={{
                padding: '4px 12px',
                fontSize: '0.9rem',
                background: isLoadingTimeData
                  ? '#666'
                  : globalTimeWindow === opt.key
                    ? saladPalette.green
                    : 'transparent',
                color: '#fff',
                border: '1px solid #999',
                borderRadius: '4px',
                cursor: isLoadingTimeData ? 'not-allowed' : 'pointer',
                transition: 'all 0.2s',
                minWidth: '32px',
                outline: 'none',
              }}
              onClick={() => {
                setIsLoadingTimeData(true);
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
          {/* Dashboard Title and Intro */}
          <Grid
            container
            spacing={4}
            sx={{ flexWrap: 'nowrap', alignItems: 'stretch' }}
            direction="row"
          >
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
              <Paper elevation={0} sx={{ p: 3, bgcolor: theme.palette.background.paper, borderRadius: 3, border: 'none', boxShadow: 'none', width: '100%' }}>
                <Typography
                  variant="body1"
                  sx={{
                    color: theme.palette.text.primary,
                    textAlign: 'justify',
                    fontSize: '1.08rem',
                    letterSpacing: 0.01,
                    lineHeight: 1.7,
                  }}
                >
                  This page provides summary statistics from SaladCloud’s testing on the Golem Network. These tests support a broader initiative to evaluate the use of the GLM token for facilitating compute transactions across SaladCloud.
                  <br /><br />
                  SaladCloud is a Web2 distributed cloud computing platform enabling customers to run workloads including text-to-image, text-to-video, molecular simulations, and zero-knowledge proofs. SaladCloud nodes are worldwide, as seen in the distribution of daily active SaladCloud nodes in the globe to the right in the full network. The data presented represents a subset of participating customers (requestors) and network nodes (providers), and includes test compute transactions executed on-chain using GLM.
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
                <GlobeComponent theme={theme} themeMode={themeMode} cityData={cityData} />
              </Box>
            </Grid>
          </Grid>

          {/* Network Content */}
          <Paper
            elevation={2}
            sx={{
              pt: 1,
              mb: 4,
              borderRadius: 4,
              px: 4,
              pb: 4,
              bgcolor: theme.palette.background.paper,
              border: `1px solid ${theme.palette.mode === 'dark' ? '#333' : '#e0e0e0'}`,
              backgroundImage: 'none',
              maxWidth: '96vw',
              mx: 'auto',
            }}
            className="w-block"
          >
            {/* Transactions Table */}
            <Box sx={{ width: '100%' }}>
              <Typography
                variant="h5"
                sx={{ mb: 0.5, color: theme.palette.primary.main, fontSize: '1.25rem' }}
              >
                Network Transactions
              </Typography>
              <Box
                sx={{
                  width: '90%',
                  borderBottom: `2px solid ${theme.palette.mode === 'dark' ? 'rgba(219,243,193,0.3)' : 'rgb(219,243,193)'}`,
                  mb: 1,
                }}
                className="w-clearfix"
              />
              <TransactionsTable data={transactions} />
            </Box>
            <Typography
              variant="h5"
              sx={{ mt: 2, mb: 0.5, color: theme.palette.primary.main, fontSize: '1.25rem' }}
              className="w-block"
            >
              Overall Usage
            </Typography>
            <Box
              sx={{
                width: '90%',
                borderBottom: `2px solid ${theme.palette.mode === 'dark' ? 'rgba(219,243,193,0.3)' : 'rgb(219,243,193)'}`,
                mb: 1,
              }}
              className="w-clearfix"
            />
            <Grid container spacing={3} justifyContent="center">
              {statsSummary ? (
                <>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-invoice-amount"
                      title="Fees ($)"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.total_invoice_amount?.map((d) => ({ x: d.ts, y: d.value })) ||
                        []
                      }
                      unit="$"
                      unitType="front"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-transaction-count"
                      title="Total Transaction Count (tx)"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.total_transaction_count?.map((d) => ({
                          x: d.ts,
                          y: d.value,
                        })) || []
                      }
                      unit="tx"
                      unitType="below"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-time-seconds"
                      title="Total Compute Time (sec)"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.total_time_seconds?.map((d) => ({ x: d.ts, y: d.value })) || []
                      }
                      unit="sec"
                      unitType="below"
                    />
                  </Grid>
                </>
              ) : (
                <Typography variant="body2" color="textSecondary">
                  Loading...
                </Typography>
              )}
            </Grid>
            <Typography
              variant="h5"
              sx={{ mt: 2, mb: 0.5, color: theme.palette.primary.main, fontSize: '1.25rem' }}
              className="w-block"
            >
              Available in Test
            </Typography>
            <Box
              sx={{
                width: '90%',
                borderBottom: `2px solid ${theme.palette.mode === 'dark' ? 'rgba(219,243,193,0.3)' : 'rgb(219,243,193)'}`,
                mb: 1,
              }}
              className="w-clearfix"
            />
            <Grid container spacing={3} justifyContent="center">
              {statsSummary ? (
                <>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="unique_node_count"
                      title="Unique Node Count"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.unique_node_count?.map((d) => ({ x: d.ts, y: d.value })) || []
                      }
                      unit=""
                      unitType="front"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="unique_node_ram"
                      title="Unique Node RAM (GB)"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.unique_node_ram?.map((d) => ({ x: d.ts, y: d.value })) || []
                      }
                      unit="GB"
                      unitType="below"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="unique_node_cpu"
                      title="Unique Node CPU (CPU cores)"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.unique_node_cpu?.map((d) => ({ x: d.ts, y: d.value })) || []
                      }
                      unit="CPU cores"
                      unitType="below"
                    />
                  </Grid>
                </>
              ) : (
                <Typography variant="body2" color="textSecondary">
                  Loading...
                </Typography>
              )}
            </Grid>
            {/* Compute Resources Utilized Section */}
            <Typography
              variant="h5"
              sx={{ mt: 4, mb: 0.5, color: theme.palette.primary.main, fontSize: '1.25rem' }}
              className="w-block"
            >
              Resource Usage
            </Typography>
            <Box
              sx={{
                width: '90%',
                borderBottom: `2px solid ${theme.palette.mode === 'dark' ? 'rgba(219,243,193,0.3)' : 'rgb(219,243,193)'}`,
                mb: 1,
              }}
              className="w-clearfix"
            />
            <Grid container spacing={3} justifyContent="center">
              <Grid size={{ xs: 12, md: 6 }}>
                {statsSummary ? (
                  <StackedChart
                    id="gpuStackedChart"
                    title="Utilized GPUs by Model"
                    trendWindow={globalTimeWindow}
                    setTrendWindow={() => {}}
                    chartData={gpuModelStackedData}
                    labels={gpuModelStackedData.groupLabels}
                    unit="nodes"
                  />
                ) : (
                  <Typography variant="body2" color="textSecondary">
                    Loading...
                  </Typography>
                )}
                <Typography
                  variant="body2"
                  color="textSecondary"
                  sx={{ textAlign: 'center', mt: 1 }}
                />
              </Grid>
              {/* VRAM StackedChart removed until data is available */}
              {statsSummary ? (
                <>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-cpu-hours"
                      title="Total CPU Hours (CPU-hr)"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.total_cpu_hours?.map((d) => ({ x: d.ts, y: d.value })) || []
                      }
                      unit="CPU-hr"
                      unitType="below"
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-ram-hours"
                      title="Total RAM Hours (GB-hr)"
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={
                        statsSummary.total_ram_hours?.map((d) => ({ x: d.ts, y: d.value })) || []
                      }
                      unit="GB-hr"
                      unitType="below"
                    />
                  </Grid>
                </>
              ) : (
                <Typography variant="body2" color="textSecondary">
                  Loading...
                </Typography>
              )}
            </Grid>
          </Paper>
        </Container>
      </Box>
    </ThemeProvider>
  );
}
