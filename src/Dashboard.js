import React, { useState, useRef, useEffect } from 'react';
import Globe from 'react-globe.gl';
import * as THREE from 'three';
import { Container, Typography, Box, Paper, Grid, ThemeProvider, createTheme, Tabs, Tab } from '@mui/material';
import { TrendChart, StackedChart } from './charts';

const theme = createTheme({
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
  const [computeCurrent, setComputeCurrent] = useState(null);
  const [feesCurrent, setFeesCurrent] = useState(null);
  const [gpuStackedCurrents, setGpuStackedCurrents] = useState([]);
  const [cpuCurrent, setCpuCurrent] = useState(null);
  const [memoryCurrent, setMemoryCurrent] = useState(null);
  const [priceCurrent, setPriceCurrent] = useState(null);
  const [volumeCurrent, setVolumeCurrent] = useState(null);
  const [marketCapCurrent, setMarketCapCurrent] = useState(null);
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
  });
  const [selectedTab, setSelectedTab] = useState(0);
  const globeNetworkRef = useRef();
  const globeSupplyRef = useRef();
  const globeBackground = '#fff';

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
  }, [selectedTab]);

return (
    <ThemeProvider theme={theme}>
        <Box sx={{ bgcolor: '#f5f5f5', minHeight: '100vh', py: 4, overflowX: 'auto' }}>
            <Container maxWidth="xl" sx={{ bgcolor: 'transparent', px: { xs: 1, sm: 2, md: 6 } }}>
              <Paper elevation={4} sx={{ bgcolor: '#1976d2', color: 'white', py: 3, mb: 4, borderRadius: 4, textAlign: 'center', maxWidth: '98vw', mx: 'auto', px: 6 }}>
                <Typography variant="h2" component="h1">Stats Salad Dashboard</Typography>
              </Paper>
          <Box sx={{ mt: 2 }}>
            <Typography variant="body1" sx={{ textAlign: 'left' }}>
              Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed euismod, urna eu tincidunt consectetur, nisi nisl aliquam enim, eget cursus enim urna euismod nisi. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Etiam at risus et justo dignissim congue. Donec congue lacinia dui, a porttitor lectus condimentum laoreet. Nunc eu ullamcorper orci. Quisque eget odio ac lectus vestibulum faucibus eget in metus. In pellentesque faucibus vestibulum. Nulla at nulla justo, eget luctus tortor. Nulla facilisi. Duis aliquet egestas purus in blandit.
            </Typography>
            <Typography variant="body1" sx={{ textAlign: 'left', mt: 2 }}>
            </Typography>
          </Box>
          <Box sx={{ mt: 3, display: 'flex', justifyContent: 'center' }}>
            <Tabs value={selectedTab} onChange={(e, v) => setSelectedTab(v)} textColor="inherit" indicatorColor="secondary">
              <Tab label="Network" />
              <Tab label="Supply" />
              <Tab label="Supplier Chefs" />
            </Tabs>
          </Box>
          {selectedTab === 0 && (
            <>
              {/* Network Section */}
              <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: '#fff', maxWidth: '96vw', mx: 'auto' }}>
                <Typography variant="h4" component="h2" sx={{ mb: 3 }}>Network</Typography>
                 {/* Distribution - utilized */}
                <Typography variant="h5" sx={{ mt: 4, mb: 2 }}>Distribution - Utilized</Typography>
                <Box sx={{ width: '90%', borderBottom: '2px solid #bbb', mb: 2 }} />
                <Box sx={{ width: '100%', height: 500, bgcolor: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', mb: 4 }} style={{ background: '#fff' }}>
                  <Globe
                    ref={globeNetworkRef}
                    width={800}
                    height={480}
                    globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
                    bumpImageUrl="//unpkg.com/three-globe/example/img/earth-topology.png"
                  />
                </Box>
                {/* Usage */}
                <Typography variant="h5" sx={{ mt: 2, mb: 2 }}>Usage</Typography>
                <Box sx={{ width: '90%', borderBottom: '2px solid #bbb', mb: 2 }} />
                <Grid container spacing={3}>
                  <Grid item xs={12} md={6}>
                    <TrendChart id="computeChart" title="Compute (hours)" trendWindow={trendWindows.compute} setTrendWindow={win => setTrendWindows(w => ({ ...w, compute: win }))} currentValue={computeCurrent} setCurrentValue={setComputeCurrent} unit="hours" unitType="below" />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TrendChart id="feesChart" title="Fees ($)" trendWindow={trendWindows.fees} setTrendWindow={win => setTrendWindows(w => ({ ...w, fees: win }))} currentValue={feesCurrent} setCurrentValue={setFeesCurrent} unit="$" unitType="front" />
                  </Grid>
                </Grid>
                {/* Resources Utilized */}
                <Typography variant="h5" sx={{ mt: 4, mb: 2 }}>Resources Utilized</Typography>
                <Box sx={{ width: '90%', borderBottom: '2px solid #bbb', mb: 2 }} />
                <Grid container spacing={3}>
                  <Grid item xs={12} md={6}>
                    <StackedChart
                      id="gpuStackedChart"
                      title="GPUs by Model"
                      trendWindow={trendWindows.gpuStacked}
                      setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuStacked: win }))}
                      labels={["5090s", "4090s", "3090s", "3060s", "Other"]}
                    />
                    <Typography variant="body2" color="textSecondary" sx={{ textAlign: 'center', mt: 1 }}>
                    </Typography>
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <StackedChart id="gpuVramChart" title="GPUs by VRAM (count)" trendWindow={trendWindows.gpuVram} setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuVram: win }))} labels={["24GB", "12GB", "8GB", "<8GB"]} />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TrendChart id="cpuChart" title="CPUs (cores)" trendWindow={trendWindows.cpu} setTrendWindow={win => setTrendWindows(w => ({ ...w, cpu: win }))} currentValue={cpuCurrent} setCurrentValue={setCpuCurrent} unit="cores" unitType="below" />
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <TrendChart id="memoryChart" title="Memory (GB)" trendWindow={trendWindows.memory} setTrendWindow={win => setTrendWindows(w => ({ ...w, memory: win }))} currentValue={memoryCurrent} setCurrentValue={setMemoryCurrent} unit="GB" unitType="below" />
                  </Grid>
                </Grid>
                {/* Token Section */}
                <Typography variant="h5" sx={{ mt: 4, mb: 2 }}>Token</Typography>
                <Box sx={{ width: '90%', borderBottom: '2px solid #bbb', mb: 2 }} />
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
            </>
          )}
          {selectedTab === 1 && (
            <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: '#fff', maxWidth: '96vw', mx: 'auto' }}>
              <Typography variant="h4" component="h2" sx={{ mb: 3 }}>Supply</Typography>
              <Typography variant="h5" sx={{ mt: 2, mb: 2 }}>Distribution - all nodes</Typography>
              <Box sx={{ width: '90%', borderBottom: '2px solid #bbb', mb: 2 }} />
              <Box sx={{ width: '100%', height: 500, bgcolor: '#fff', display: 'flex', alignItems: 'center', justifyContent: 'center', mb: 4 }} style={{ background: '#fff' }}>
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
                  <StackedChart id="supplyGpuModelChart" title="GPUs by Model" trendWindow={trendWindows.gpuStacked} setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuStacked: win }))} labels={["5090s", "4090s", "3090s", "3060s", "Other"]} />
                  <Typography variant="body2" color="textSecondary" sx={{ textAlign: 'center', mt: 1 }}>
                  </Typography>
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="supplyGpuVramChart" title="GPUs by VRAM (count)" trendWindow={trendWindows.gpuVram} setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuVram: win }))} labels={["24GB", "12GB", "8GB", "<8GB"]} />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="supplyCpuChart" title="CPUs (cores)" trendWindow={trendWindows.cpu} setTrendWindow={win => setTrendWindows(w => ({ ...w, cpu: win }))} labels={["Running", "Available"]} />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="supplyMemoryChart" title="Memory (GB)" trendWindow={trendWindows.memory} setTrendWindow={win => setTrendWindows(w => ({ ...w, memory: win }))} labels={["Running", "Available"]} />
                </Grid>
              </Grid>
            </Paper>
          )}
          {selectedTab === 2 && (
            <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: '#fff', maxWidth: '96vw', mx: 'auto' }}>
              <Typography variant="h4" component="h2" sx={{ mb: 3 }}>Supplier Chefs</Typography>
              <Grid container spacing={3}>
                <Grid item xs={12} md={6}>
                  <TrendChart id="meanEarningsChart" title="Mean Earnings per Machine (8hr min)" trendWindow={trendWindows.meanEarnings ?? 'month'} setTrendWindow={win => setTrendWindows(w => ({ ...w, meanEarnings: win }))} currentValue={null} setCurrentValue={() => {}} unit="$" unitType="front" />
                </Grid>
                <Grid item xs={12} md={6}>
                  <StackedChart id="earningsByGpuChart" title="Earnings by GPU ($)" trendWindow={trendWindows.earningsByGpu ?? 'month'} setTrendWindow={win => setTrendWindows(w => ({ ...w, earningsByGpu: win }))} labels={["5090s", "4090s", "3090s", "3060s", "Other GPU", "Non-GPU workloads"]} />
                </Grid>
              </Grid>
            </Paper>
          )}
            </Container>
        </Box>
    </ThemeProvider>
);
}
