// Fast-loading hook for metrics bar totals
function useStatsTotals(period = 'week', gpu = 'all') {
  const [totals, setTotals] = useState(null);
  useEffect(() => {
    fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/stats?period=${period}&gpu=${gpu}`)
      .then((res) => (res.ok ? res.json() : Promise.reject('Failed to fetch stats totals')))
      .then((data) => setTotals(data))
      .catch((err) => {
        console.error('Error loading stats totals:', err);
        setTotals(null);
      });
  }, [period, gpu]);
  return totals;
}
// Dashboard.jsx - Main dashboard component for Stats Salad
// Uses Material-UI, Chart.jsx, react-globe.gl, and custom chart components

import React, { useState, useRef, useEffect, useMemo } from 'react';
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
} from '@mui/material';
import { TrendChart, StackedChart } from './charts.jsx';
import MetricsBar from './MetricsBar.jsx';
import TransactionsTable from './TransactionsTable.jsx';
import GlobeComponent from './Globe.jsx';

// Custom color palette
const saladPalette = {
  green: 'rgb(83,166,38)', // Main accent green
  darkGreen: 'rgb(31,79,34)', // Darker green
  lime: 'rgb(178,213,48)', // Lime accent
  midGreen: 'rgb(120,200,60)', // Midway green for globe bars in dark mode
  lightGreen: 'rgb(219,243,193)', // Light green background
  navy: 'rgb(10,33,51)', // Deep navy for backgrounds
};

// StyledPaper: reusable Paper with common dashboard styles
function StyledPaper({ children, sx = {}, ...props }) {
  // theme is available via ThemeProvider context
  // Use function form for sx to access theme
  return (
    <Paper
      elevation={2}
      sx={(theme) => ({
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
        ...(typeof sx === 'function' ? sx(theme) : sx),
      })}
      className="w-block"
      {...props}
    >
      {children}
    </Paper>
  );
}

// StyledHeading: reusable heading with border line
function StyledHeading({ children, mt = 2, ...props }) {
  return (
    <>
      <Typography
        variant="h5"
        sx={(theme) => ({
          mt,
          mb: 0.5,
          color: theme.palette.primary.main,
          fontSize: '1.75rem',
        })}
        className="w-block"
        {...props}
      >
        {children}
      </Typography>
      <Box
        sx={(theme) => ({
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
  const [isLoading, setIsLoading] = useState(false);
  // Track which time window button is loading
  const [loadingTimeWindow, setLoadingTimeWindow] = useState(null);
  // Theme mode state with persistence
  const [themeMode, setThemeMode] = useState(getInitialTheme);

  // Save theme to localStorage when it changes
  useEffect(() => {
    localStorage.setItem('theme', themeMode);
  }, [themeMode]);

  // Memoize theme so it only changes when themeMode changes
  const theme = useMemo(() => createAppTheme(themeMode), [themeMode]);

  // Helper to fetch stats summary for the bottom section (trends)
  function useStatsSummary(period = 'month', gpu = 'all') {
    const [stats, setStats] = useState(null);
    useEffect(() => {
      setIsLoading(true);
      fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/trends?period=${period}&gpu=${gpu}`)
        .then((res) => (res.ok ? res.json() : Promise.reject('Failed to fetch stats')))
        .then((data) => {
          setStats(data);
          setIsLoading(false);
          setLoadingTimeWindow(null);
        })
        .catch((err) => {
          console.error('Error loading stats summary:', err);
          setStats(null);
          setIsLoading(false);
          setLoadingTimeWindow(null);
        });
    }, [period, gpu]);
    return stats;
  }

  // Fast totals for metrics bar
  const statsTotals = useStatsTotals(globalTimeWindow, 'all');
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

  // Load node count data from API endpoint only once on mount
  useEffect(() => {
    console.log("Fetching globe cityData...");
    fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/geo_counts`)
      .then((res) => (res.ok ? res.json() : Promise.reject('Failed to fetch city data')))
      .then((data) => {
          setCityData(data);
      })
      .catch((err) => {
        console.error('Error loading cityData:', err);
        setCityData([]);
      });
  }, []); // Only run on mount, not on time window change

  // Helper function to transform backend data for stacked charts
  const createStackedChartData = React.useCallback((statsSummary, fieldName) => {
    const legendColors = ['#b2d530', '#9acc35', '#7bb82e', '#53a626', '#3d6b28', '#1f4f22'];
    const raw = statsSummary?.[fieldName];
    if (!raw || !raw.labels || !raw.datasets) {
      return { labels: [], datasets: [] };
    }
    // Calculate frequency (sum of values) for each dataset
    const datasetsWithFrequency = raw.datasets.map((ds, i) => {
      const freq = Array.isArray(ds.data) ? ds.data.reduce((a, b) => a + Math.abs(b), 0) : 0;
      return {
        ...ds,
        _frequency: freq,
        _originalIndex: i,
      };
    });
    // Sort datasets by frequency descending
    const sortedDatasets = datasetsWithFrequency
      .sort((a, b) => b._frequency - a._frequency)
      .map((ds, i) => ({
        ...ds,
        backgroundColor: legendColors[i % legendColors.length],
        borderColor: '#fff',
        borderWidth: 1,
        fill: true,
      }));
    return { labels: raw.labels, datasets: sortedDatasets };
  }, []);

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <GlobalStyles
        styles={{
          '@font-face': {
            fontFamily: 'webflow-icons',
            src: "url(data:application/x-font-ttf;charset=utf-8;base64,AAEAAAALAIAAAwAwT1MvMg8SBiUAAAC8AAAAYGNtYXDpP+a4AAABHAAAAFxnYXNwAAAAEAAAAXgAAAAIZ2x5ZmhS2XEAAAGAAAADHGhlYWQTFw3HAAAEnAAAADZoaGVhCXYFgQAABNQAAAAkaG10eCe4A1oAAAT4AAAAMGxvY2EDtALGAAAFKAAAABptYXhwABAAPgAABUQAAAAgbmFtZSoCsMsAAAVkAAABznBvc3QAAwAAAAAHNAAAACAAAwP4AZAABQAAApkCzAAAAI8CmQLMAAAB6wAzAQkAAAAAAAAAAAAAAAAAAAABEAAAAAAAAAAAAAAAAAAAAABAAADpAwPA/8AAQAPAAEAAAAABAAAAAAAAAAAAAAAgAAAAAAADAAAAAwAAABwAAQADAAAAHAADAAEAAAAcAAQAQAAAAAwACAACAAQAAQAg5gPpA//9//8AAAAAACDmAOkA//3//wAB/+MaBBcIAAMAAQAAAAAAAAAAAAAAAAABAAH//wAPAAEAAAAAAAAAAAACAAA3OQEAAAAAAQAAAAAAAAAAAAIAADc5AQAAAAABAAAAAAAAAAAAAgAANzkBAAAAAAEBIAAAAyADgAAFAAAJAQcJARcDIP5AQAGA/oBAAcABwED+gP6AQAABAOAAAALgA4AABQAAEwEXCQEH4AHAQP6AAYBAAcABwED+gP6AQAAAAwDAAOADQALAAA8AHwAvAAABISIGHQEUFjMhMjY9ATQmByEiBh0BFBYzITI2PQE0JgchIgYdARQWMyEyNj0BNCYDIP3ADRMTDQJADRMTDf3ADRMTDQJADRMTDf3ADRMTDQJADRMTAsATDSANExMNIA0TwBMNIA0TEw0gDRPAEw0gDRMTDSANEwAAAAABAJ0AtAOBApUABQAACQIHCQEDJP7r/upcAXEBcgKU/usBFVz+fAGEAAAAAAL//f+9BAMDwwAEAAkAABcBJwEXAwE3AQdpA5ps/GZsbAOabPxmbEMDmmz8ZmwDmvxmbAOabAAAAgAA/8AEAAPAAB0AOwAABSInLgEnJjU0Nz4BNzYzMTIXHgEXFhUUBw4BBwYjNTI3PgE3NjU0Jy4BJyYjMSIHDgEHBhUUFx4BFxYzAgBqXV6LKCgoKIteXWpqXV6LKCgoKIteXWpVSktvICEhIG9LSlVVSktvICEhIG9LSlVAKCiLXl1qal1eiygoKCiLXl1qal1eiygoZiEgb0tKVVVKS28gISEgb0tKVVVKS28gIQABAAABwAIAA8AAEgAAEzQ3PgE3NjMxFSIHDgEHBhUxIwAoKIteXWpVSktvICFmAcBqXV6LKChmISBvS0pVAAAAAgAA/8AFtgPAADIAOgAAARYXHgEXFhUUBw4BBwYHIxUhIicuAScmNTQ3PgE3NjMxOAExNDc+ATc2MzIXHgEXFhcVATMJATMVMzUEjD83NlAXFxYXTjU1PQL8kz01Nk8XFxcXTzY1PSIjd1BQWlJJSXInJw3+mdv+2/7c25MCUQYcHFg5OUA/ODlXHBwIAhcXTzY1PTw1Nk8XF1tQUHcjIhwcYUNDTgL+3QFt/pOTkwABAAAAAQAAmM7nP18PPPUACwQAAAAAANciZKUAAAAA1yJkpf/9/70FtgPDAAAACAACAAAAAAAAAAEAAAPA/8AAAAW3//3//QW2AAEAAAAAAAAAAAAAAAAAAAAMBAAAAAAAAAAAAAAAAgAAAAQAASAEAADgBAAAwAQAAJ0EAP/9BAAAAAQAAAAFtwAAAAAAAAAKABQAHgAyAEYAjACiAL4BFgE2AY4AAAABAAAADAA8AAMAAAAAAAIAAAAAAAAAAAAAAAAAAAAAAAAADgCuAAEAAAAAAAEADQAAAAEAAAAAAAIABwCWAAEAAAAAAAMADQBIAAEAAAAAAAQADQCrAAEAAAAAAAUACwAnAAEAAAAAAAYADQBvAAEAAAAAAAoAGgDSAAMAAQQJAAEAGgANAAMAAQQJAAIADgCdAAMAAQQJAAMAGgBVAAMAAQQJAAQAGgC4AAMAAQQJAAUAFgAyAAMAAQQJAAYAGgB8AAMAAQQJAAoANADsd2ViZmxvdy1pY29ucwB3AGUAYgBmAGwAbwB3AC0AaQBjAG8AbgBzd2ViZmxvdy1pY29ucwB3AGUAYgBmAGwAbwB3AC0AaQBjAG8AbnNGb250IGdlbmVyYXRlZCBieSBJY29Nb29uLgBGAW9uAHQAZwBlAG4AZQByAGEAdABlAGQAIABiAHkgSQBjAG8ATQBvAG8AbgAuAAAAAAwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA==) format('truetype')",
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
            { key: 'week', label: '7d' },
            { key: 'two_weeks', label: '14d' },
            { key: 'month', label: '31d' },
          ].map((opt) => (
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
          {statsTotals &&
            (() => {
              const periodLabel =
                globalTimeWindow === 'day'
                  ? 'last day'
                  : globalTimeWindow === 'week'
                    ? 'last week'
                    : 'last month';
              return (
                <MetricsBar
                  key={globalTimeWindow + '-' + Object.values(statsTotals).join('-')}
                  metrics={[
                    {
                      value: statsTotals.unique_node_count ?? 0,
                      unit: '',
                      label: `Active nodes (${periodLabel})`,
                    },
                    {
                      value: statsTotals.total_invoice_amount ?? 0,
                      unit: '$',
                      label: `Fees paid (${periodLabel})`,
                    },
                    {
                      value: statsTotals.total_time_hours ?? 0,
                      unit: 'hours',
                      label: `Compute time (${periodLabel})`,
                    },
                    {
                      value: statsTotals.total_transaction_count ?? 0,
                      unit: '',
                      label: `Transactions (${periodLabel})`,
                    },
                    // Add more metrics as needed
                  ]}
                />
              );
            })()}
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
                  This page provides summary statistics from SaladCloud’s testing on the Golem
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
                <GlobeComponent theme={theme} themeMode={themeMode} cityData={cityData} />
              </Box>
            </Grid>
          </Grid>

          <StyledPaper sx={{ pb: 0 }}>
            {/* Transactions Table */}
            <Box sx={{ width: '100%' }}>
              <StyledHeading>Network Transactions</StyledHeading>
              <TransactionsTable data={transactions} />
            </Box>
          </StyledPaper>
          <StyledPaper>
            <StyledHeading>Network Activity</StyledHeading>
            <Grid container spacing={3} justifyContent="center">
              {statsSummary ? (
                <>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="unique_node_count"
                      title="Active Unique Nodes"
                      description="Number of SaladCloud providers that ran workloads."
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={statsSummary.unique_node_count || []}
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
                      setTrendWindow={() => {}}
                      trendData={statsSummary.total_invoice_amount || []}
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
                      setTrendWindow={() => {}}
                      trendData={statsSummary.total_time_hours || []}
                      unit="hours"
                      unitType="below"
                      isLoading={isLoading}
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-transaction-count"
                      title="Transaction Count"
                      description="Total number of compute transactions between customers and providers."
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={statsSummary.total_transaction_count || []}
                      unit=""
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
              {statsSummary ? (
                <>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    <TrendChart
                      id="trend-total-ram-hours"
                      title="Memory (GB-hr)"
                      description="Aggregated RAM usage across all workloads and provider nodes."
                      trendWindow={globalTimeWindow}
                      setTrendWindow={() => {}}
                      trendData={statsSummary.total_ram_hours || []}
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
                      setTrendWindow={() => {}}
                      trendData={statsSummary.total_cpu_hours || []}
                      unit="CPU-hr"
                      unitType="below"
                      isLoading={isLoading}
                    />
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    {(() => {
                      const gpuData = createStackedChartData(statsSummary, 'gpu_unique_node_count');
                      return (
                        <StackedChart
                          id="gpuStackedChart"
                          title="GPUs Used by Model"
                          description="Provider nodes running GPU workloads, by model."
                          trendWindow={globalTimeWindow}
                          setTrendWindow={() => {}}
                          chartData={gpuData}
                          labels={gpuData.labels}
                          unit="nodes"
                        />
                      );
                    })()}
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    {(() => {
                      const gpuVramData = createStackedChartData(
                        statsSummary,
                        'vram_unique_node_count',
                      );
                      return (
                        <StackedChart
                          id="gpuStackedChartVram"
                          title="GPUs Used by VRAM"
                          description="Provider nodes running GPU workloads, by VRAM."
                          trendWindow={globalTimeWindow}
                          setTrendWindow={() => {}}
                          chartData={gpuVramData}
                          labels={gpuVramData.labels}
                          unit="nodes"
                        />
                      );
                    })()}
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    {(() => {
                      const gpuTimeData = createStackedChartData(
                        statsSummary,
                        'gpu_total_time_hours',
                      );
                      return (
                        <StackedChart
                          id="gpuStackedChartTime"
                          title="GPUs Time by Model (hr)"
                          description="Time GPU customer workloads were running on SaladCloud, by GPU."
                          trendWindow={globalTimeWindow}
                          setTrendWindow={() => {}}
                          chartData={gpuTimeData}
                          labels={gpuTimeData.labels}
                          unit="hours"
                        />
                      );
                    })()}
                  </Grid>
                  <Grid size={{ xs: 12, sm: 6 }}>
                    {(() => {
                      const gpuVramTimeData = createStackedChartData(
                        statsSummary,
                        'vram_total_time_hours',
                      );
                      return (
                        <StackedChart
                          id="gpuStackedChartVramTime"
                          title="GPUs Time by VRAM (hr)"
                          description="Time GPU customer workloads were running on SaladCloud, by VRAM."
                          trendWindow={globalTimeWindow}
                          setTrendWindow={() => {}}
                          chartData={gpuVramTimeData}
                          labels={gpuVramTimeData.labels}
                          unit="hours"
                        />
                      );
                    })()}
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
