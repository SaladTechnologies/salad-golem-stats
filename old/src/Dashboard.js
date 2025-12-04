// Dashboard.js - Main dashboard component for Stats Salad
// Uses Material-UI, Chart.js, react-globe.gl, and custom chart components

import React, { useState, useRef, useEffect } from 'react';
import Globe from 'react-globe.gl';
import * as THREE from 'three';
import { Container, Typography, Box, Paper, Grid, ThemeProvider, createTheme, Tabs, Tab, CssBaseline, GlobalStyles } from '@mui/material';
import { TrendChart, StackedChart } from './charts';

// Custom color palette
const saladPalette = {
  green: 'rgb(83,166,38)',        // Main accent green
  darkGreen: 'rgb(31,79,34)',    // Darker green
  lime: 'rgb(178,213,48)',       // Lime accent
  lightGreen: 'rgb(219,243,193)',// Light green background
  navy: 'rgb(10,33,51)',         // Deep navy for backgrounds
};

// Material-UI theme with custom palette
const theme = createTheme({
  palette: {
    primary: {
      main: saladPalette.green,
      dark: saladPalette.darkGreen,
      light: saladPalette.lime,
      contrastText: '#fff',
    },
    secondary: {
      main: saladPalette.lime,
      dark: saladPalette.green,
      light: saladPalette.lightGreen,
      contrastText: saladPalette.navy,
    },
    background: {
      default: '#fff',
      paper: '#fff',
    },
    text: {
      primary: saladPalette.navy,
      secondary: saladPalette.darkGreen,
    },
    salad: saladPalette, // custom for direct access
  },
  typography: {
    fontFamily: 'ui-sans-serif, system-ui, sans-serif, Apple Color Emoji, Segoe UI Emoji, Segoe UI Symbol, Noto Color Emoji',
    fontSize: 14,
    lineHeight: '20px',
    h1: { fontWeight: 700, fontSize: 38, lineHeight: '44px', marginTop: 20, marginBottom: 10 },
    h2: { fontWeight: 700, fontSize: 32, lineHeight: '36px', marginTop: 20, marginBottom: 10 },
    h3: { fontWeight: 700, fontSize: 24, lineHeight: '30px', marginTop: 20, marginBottom: 10 },
    h4: { fontWeight: 700, fontSize: 18, lineHeight: '24px', marginTop: 10, marginBottom: 10 },
    h5: { fontWeight: 700, fontSize: 24, lineHeight: '30px', marginTop: 16, marginBottom: 12 },
    h6: { fontWeight: 700, fontSize: 18, lineHeight: '24px', marginTop: 10, marginBottom: 10 },
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
  // State for current values in charts
  const [computeCurrent, setComputeCurrent] = useState(null);
  const [feesCurrent, setFeesCurrent] = useState(null);
  const [cpuCurrent, setCpuCurrent] = useState(null);
  const [memoryCurrent, setMemoryCurrent] = useState(null);
  const [priceCurrent, setPriceCurrent] = useState(null);
  const [volumeCurrent, setVolumeCurrent] = useState(null);
  const [marketCapCurrent, setMarketCapCurrent] = useState(null);

  // State for selected time window for each chart
  const [trendWindows, setTrendWindows] = useState({
    compute: 'month',
    fees: 'month',
    gpuStacked: 'month',
    gpuVram: 'month',
    cpu: 'month',
    memory: 'month',
    price: 'month',
    volume: 'month',
    marketCap: 'month',
    meanEarnings: 'month',
    earningsByGpu: 'month',
  });

  // Tab selection state
  const [selectedTab, setSelectedTab] = useState(0);

  // Globe refs for Network and Supply
  const globeNetworkRef = useRef();
  const globeSupplyRef = useRef();

  // Ensure globe backgrounds are white
  useEffect(() => {
    if (selectedTab === 0 && globeNetworkRef.current && globeNetworkRef.current.scene) {
      const scene = globeNetworkRef.current.scene();
      if (scene) scene.background = new THREE.Color('#fff');
    }
  }, [selectedTab, globeNetworkRef]);

  useEffect(() => {
    if (globeSupplyRef.current && globeSupplyRef.current.scene) {
      const scene = globeSupplyRef.current.scene();
      if (scene) scene.background = new THREE.Color('#fff');
    }
  }, [selectedTab, globeSupplyRef]);

  // Chart label arrays for reuse
  const gpuModelLabels = ["5090s", "4090s", "3090s", "3060s", "Other"];
  const gpuVramLabels = ["24GB", "12GB", "8GB", "<8GB"];
  const cpuMemLabels = ["Running", "Available"];
  const earningsGpuLabels = ["5090s", "4090s", "3090s", "3060s", "Other GPU", "Non-GPU workloads"];

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <GlobalStyles styles={{
        '@font-face': {
          fontFamily: 'webflow-icons',
          src: "url(data:application/x-font-ttf;charset=utf-8;base64,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) format('truetype')",
          fontWeight: 400,
          fontStyle: 'normal',
        },
        '*': {
          boxSizing: 'border-box',
        },
        'body': {
          margin: 0,
          minHeight: '100%',
          backgroundColor: '#fff',
          fontFamily: 'Arial, sans-serif',
          fontSize: '14px',
          lineHeight: '20px',
          color: '#333',
        },
        'img': {
          maxWidth: '100%',
          verticalAlign: 'middle',
          display: 'inline-block',
        },
        'h1': { fontWeight: 700, fontSize: '38px', lineHeight: '44px', marginTop: '20px', marginBottom: '10px' },
        'h2': { fontWeight: 700, fontSize: '32px', lineHeight: '36px', marginTop: '20px', marginBottom: '10px' },
        'h3': { fontWeight: 700, fontSize: '24px', lineHeight: '30px', marginTop: '20px', marginBottom: '10px' },
        'h4': { fontWeight: 700, fontSize: '18px', lineHeight: '24px', marginTop: '10px', marginBottom: '10px' },
        'h5': { fontWeight: 700, fontSize: '14px', lineHeight: '20px', marginTop: '10px', marginBottom: '10px' },
        'h6': { fontWeight: 700, fontSize: '12px', lineHeight: '18px', marginTop: '10px', marginBottom: '10px' },
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
      }} />
      {/* Top nav bar with logo */}
      <Box sx={{ height: 64, bgcolor: theme.palette.salad.navy, display: 'flex', alignItems: 'center', px: 3, borderBottom: '4px solid theme.palette.salad.navy' }} className="w-block">
        <a href="https://portal.salad.com" target="_blank" rel="noopener noreferrer" style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
          <img src="/salad-logo-light-4x-DpLeJfaD.png" alt="Salad Logo" style={{ height: 46.15, width: 'auto', display: 'block' }} />
        </a>
      </Box>
      <Box sx={{ bgcolor: theme.palette.background.default, minHeight: '100vh', py: 4, overflowX: 'auto' }}>
        <Container maxWidth="xl" sx={{ bgcolor: 'transparent', px: { xs: 1, sm: 2, md: 6 } }}>
          {/* Dashboard Title and Intro */}
          <Box sx={{ mb: 4, mt: 2 }} className="w-block">
            <Typography variant="h2" component="h1" sx={{ color: theme.palette.text.primary, textAlign: 'left', fontWeight: 700 }} className="w-block">Salad Network Statistics</Typography>
          </Box>
          <Box sx={{ mt: 2 }} className="w-block">
            <Typography variant="body1" sx={{ textAlign: 'left', color: theme.palette.text.primary }} className="w-block">
              {/* Introductory lorem ipsum text */}
              Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed euismod, urna eu tincidunt consectetur, nisi nisl aliquam enim, eget cursus enim urna euismod nisi. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Etiam at risus et justo dignissim congue. Donec congue lacinia dui, a porttitor lectus condimentum laoreet. Nunc eu ullamcorper orci. Quisque eget odio ac lectus vestibulum faucibus eget in metus. In pellentesque faucibus vestibulum. Nulla at nulla justo, eget luctus tortor. Nulla facilisi. Duis aliquet egestas purus in blandit.
            </Typography>
            <Typography variant="body1" sx={{ textAlign: 'left', mt: 2, color: theme.palette.text.primary }} className="w-block">
              {/* Visible return/line break for spacing */}
            </Typography>
          </Box>
          {/* Tab Navigation */}
          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'center' }} className="w-block">
            <Tabs value={selectedTab} onChange={(e, v) => setSelectedTab(v)}
              textColor="inherit"
              indicatorColor="secondary"
              TabIndicatorProps={{ style: { background: theme.palette.secondary.main } }}
              sx={{
                '.MuiTab-root': {
                  color: theme.palette.primary.dark,
                  fontWeight: 600,
                },
                '.Mui-selected': {
                  color: theme.palette.primary.main + ' !important',
                },
              }}
            >
              <Tab label="Network" className="w-inline-block" />
              <Tab label="Supply" className="w-inline-block" />
              <Tab label="Supplier Chefs" className="w-inline-block" />
            </Tabs>
          </Box>

          {/* Network Tab Content */}
          {selectedTab === 0 && (
            <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: theme.palette.background.paper, maxWidth: '96vw', mx: 'auto' }} className="w-block">
              <Typography variant="h4" component="h2" sx={{ mb: 3, color: theme.palette.primary.dark, fontSize: '2rem' }} className="w-block">Network</Typography>
              {/* Distribution Section (moved to top) */}
              <Typography variant="h5" sx={{ mt: 2, mb: 1, color: theme.palette.primary.main, fontSize: '1.25rem' }} className="w-block">Distribution - Utilized</Typography>
              <Box sx={{ width: '90%', borderBottom: '2px solid rgb(219,243,193)', mb: 1 }} className="w-clearfix" />
              <Box sx={{ width: '100%', height: 500, bgcolor: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', mb: 4 }} style={{ background: '#fff' }} className="w-block">
                <Globe
                  ref={globeNetworkRef}
                  width={800}
                  height={480}
                  globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
                  bumpImageUrl="//unpkg.com/three-globe/example/img/earth-topology.png"
                />
              </Box>
              {/* Usage Section */}
              <Typography variant="h5" sx={{ mt: 2, mb: 1, color: theme.palette.primary.main, fontSize: '1.25rem' }} className="w-block">Usage</Typography>
              <Box sx={{ width: '90%', borderBottom: '2px solid rgb(219,243,193)', mb: 1 }} className="w-clearfix" />
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <TrendChart id="computeChart" title="Compute (hours)" trendWindow={trendWindows.compute} setTrendWindow={win => setTrendWindows(w => ({ ...w, compute: win }))} currentValue={computeCurrent} setCurrentValue={setComputeCurrent} unit="hours" unitType="below" />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TrendChart id="feesChart" title="Fees ($)" trendWindow={trendWindows.fees} setTrendWindow={win => setTrendWindows(w => ({ ...w, fees: win }))} currentValue={feesCurrent} setCurrentValue={setFeesCurrent} unit="$" unitType="front" />
                </Grid>
              </Grid>
              {/* Resources Utilized Section */}
              <Typography variant="h5" sx={{ mt: 4, mb: 1, color: theme.palette.primary.main, fontSize: '1.25rem' }} className="w-block">Resources Utilized</Typography>
              <Box sx={{ width: '90%', borderBottom: '2px solid rgb(219,243,193)', mb: 1 }} className="w-clearfix" />
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <StackedChart id="gpuStackedChart" title="GPUs by Model" trendWindow={trendWindows.gpuStacked} setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuStacked: win }))} labels={gpuModelLabels} />
                  <Typography variant="body2" color="textSecondary" sx={{ textAlign: 'center', mt: 1 }} />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="gpuVramChart" title="GPUs by VRAM (count)" trendWindow={trendWindows.gpuVram} setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuVram: win }))} labels={gpuVramLabels} />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TrendChart id="cpuChart" title="CPUs (cores)" trendWindow={trendWindows.cpu} setTrendWindow={win => setTrendWindows(w => ({ ...w, cpu: win }))} currentValue={cpuCurrent} setCurrentValue={setCpuCurrent} unit="cores" unitType="below" />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TrendChart id="memoryChart" title="Memory (GB)" trendWindow={trendWindows.memory} setTrendWindow={win => setTrendWindows(w => ({ ...w, memory: win }))} currentValue={memoryCurrent} setCurrentValue={setMemoryCurrent} unit="GB" unitType="below" />
                </Grid>
              </Grid>
              {/* Token Section */}
              <Typography variant="h5" sx={{ mt: 4, mb: 1, color: theme.palette.primary.main, fontSize: '1.25rem' }} className="w-block">Token</Typography>
              <Box sx={{ width: '90%', borderBottom: '2px solid rgb(219,243,193)', mb: 1 }} className="w-clearfix" />
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <TrendChart id="priceChart" title="$ Price" trendWindow={trendWindows.price} setTrendWindow={win => setTrendWindows(w => ({ ...w, price: win }))} currentValue={priceCurrent} setCurrentValue={setPriceCurrent} unit="$" unitType="front" />
                </Grid>
                <Grid item xs={12} md={6}>
                  <TrendChart id="marketCapChart" title="Market Capitalization" trendWindow={trendWindows.marketCap} setTrendWindow={win => setTrendWindows(w => ({ ...w, marketCap: win }))} currentValue={marketCapCurrent} setCurrentValue={setMarketCapCurrent} unit="$" unitType="front" />
                </Grid>
                <Grid item xs={12} md={6}>
                  <Box sx={{ width: { xs: '100%', sm: 600 }, minWidth: 300 }}>
                    <TrendChart id="volumeChart" title="Volume" trendWindow={trendWindows.volume} setTrendWindow={win => setTrendWindows(w => ({ ...w, volume: win }))} currentValue={volumeCurrent} setCurrentValue={setVolumeCurrent} />
                  </Box>
                </Grid>
                <Grid item xs={12} md={6}></Grid>
              </Grid>
            </Paper>
          )}

          {/* Supply Tab Content */}
          {selectedTab === 1 && (
            <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: theme.palette.background.paper, maxWidth: '96vw', mx: 'auto' }} className="w-block">
              <Typography variant="h4" component="h2" sx={{ mb: 3, color: theme.palette.primary.dark, fontSize: '2rem' }} className="w-block">Supply</Typography>
              <Typography variant="h5" sx={{ mt: 2, mb: 1, color: theme.palette.primary.main, fontSize: '1.25rem' }} className="w-block">Distribution - all nodes</Typography>
              <Box sx={{ width: '90%', borderBottom: '2px solid rgb(219,243,193)', mb: 1 }} className="w-clearfix" />
              <Box sx={{ width: '100%', height: 500, bgcolor: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', mb: 4 }} style={{ background: '#fff' }} className="w-block">
                <Globe
                  ref={globeSupplyRef}
                  width={800}
                  height={480}
                  globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
                  bumpImageUrl="//unpkg.com/three-globe/example/img/earth-topology.png"
                />
              </Box>
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <StackedChart id="supplyGpuModelChart" title="GPUs by Model" trendWindow={trendWindows.gpuStacked} setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuStacked: win }))} labels={gpuModelLabels} />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="supplyGpuVramChart" title="GPUs by VRAM (count)" trendWindow={trendWindows.gpuVram} setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuVram: win }))} labels={gpuVramLabels} />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="supplyCpuChart" title="CPUs (cores)" trendWindow={trendWindows.cpu} setTrendWindow={win => setTrendWindows(w => ({ ...w, cpu: win }))} labels={cpuMemLabels} />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="supplyMemoryChart" title="Memory (GB)" trendWindow={trendWindows.memory} setTrendWindow={win => setTrendWindows(w => ({ ...w, memory: win }))} labels={cpuMemLabels} />
                </Grid>
              </Grid>
            </Paper>
          )}

          {/* Supplier Chefs Tab Content */}
          {selectedTab === 2 && (
            <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: theme.palette.background.paper, maxWidth: '96vw', mx: 'auto' }} className="w-block">
              <Typography variant="h4" component="h2" sx={{ mb: 3, color: theme.palette.primary.dark, fontSize: '2rem' }} className="w-block">Supplier Chefs</Typography>
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <TrendChart id="meanEarningsChart" title="Mean Earnings per Machine (8hr min)" trendWindow={trendWindows.meanEarnings} setTrendWindow={win => setTrendWindows(w => ({ ...w, meanEarnings: win }))} currentValue={null} setCurrentValue={() => {}} unit="$" unitType="front" />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="earningsByGpuChart" title="Earnings by GPU ($)" trendWindow={trendWindows.earningsByGpu} setTrendWindow={win => setTrendWindows(w => ({ ...w, earningsByGpu: win }))} labels={earningsGpuLabels} />
                </Grid>
              </Grid>
            </Paper>
          )}
        </Container>
      </Box>
    </ThemeProvider>
  );
}
