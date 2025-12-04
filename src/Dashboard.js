import React, { useState, useRef } from 'react';
import Globe from 'react-globe.gl';
import * as THREE from 'three';
import { Container, Typography, Box, Paper, Grid, ThemeProvider, createTheme } from '@mui/material';
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
  const globeRef = useRef();

  React.useEffect(() => {
    if (globeRef.current && globeRef.current.scene) {
      const scene = globeRef.current.scene();
      if (scene && scene.background) {
        scene.background.set('#f5f5f5');
      } else if (scene) {
        scene.background = new THREE.Color('#f5f5f5');
      }
    }
  }, []);

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
                {/* Network Section */}
                <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: '#fff', maxWidth: '96vw', mx: 'auto' }}>
                    <Typography variant="h4" component="h2" sx={{ mb: 3 }}>Network</Typography>
                    {/* Usage */}
                    <Typography variant="h5" sx={{ mt: 2, mb: 2 }}>Usage</Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                            <TrendChart id="computeChart" title="Compute (hours)" trendWindow={trendWindows.compute} setTrendWindow={win => setTrendWindows(w => ({ ...w, compute: win }))} currentValue={computeCurrent} setCurrentValue={setComputeCurrent} />
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <TrendChart id="feesChart" title="Fees ($)" trendWindow={trendWindows.fees} setTrendWindow={win => setTrendWindows(w => ({ ...w, fees: win }))} currentValue={feesCurrent} setCurrentValue={setFeesCurrent} />
                        </Grid>
                    </Grid>
                    {/* Resources Utilized */}
                    <Typography variant="h5" sx={{ mt: 4, mb: 2 }}>Resources Utilized</Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                            <StackedChart
                                id="gpuStackedChart"
                                title="GPUs by Model"
                                trendWindow={trendWindows.gpuStacked}
                                setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuStacked: win }))}
                                stackedCurrents={gpuStackedCurrents}
                                setStackedCurrents={setGpuStackedCurrents}
                                labels={["5090s", "4090s", "3090s", "3060s", "Other"]}
                            />
                            <Typography variant="body2" color="textSecondary" sx={{ textAlign: 'center', mt: 1 }}>
                            </Typography>
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <StackedChart
                                id="gpuVramChart"
                                title="GPUs by VRAM (count)"
                                trendWindow={trendWindows.gpuVram}
                                setTrendWindow={win => setTrendWindows(w => ({ ...w, gpuVram: win }))}
                                labels={["24GB", "12GB", "8GB", "<8GB"]}
                            />
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <TrendChart id="cpuChart" title="CPUs (cores)" trendWindow={trendWindows.cpu} setTrendWindow={win => setTrendWindows(w => ({ ...w, cpu: win }))} currentValue={cpuCurrent} setCurrentValue={setCpuCurrent} />
                        </Grid>
                        <Grid item xs={12} md={6}>
                            <TrendChart id="memoryChart" title="Memory (GB)" trendWindow={trendWindows.memory} setTrendWindow={win => setTrendWindows(w => ({ ...w, memory: win }))} currentValue={memoryCurrent} setCurrentValue={setMemoryCurrent} />
                        </Grid>
                    </Grid>
                    {/* Distribution - utilized */}
                    <Typography variant="h5" sx={{ mt: 4, mb: 2 }}>Distribution - Utilized</Typography>
                    <Box sx={{ width: '100%', height: 500, bgcolor: '#e3e3e3', display: 'flex', alignItems: 'center', justifyContent: 'center', mb: 4 }}>
                        <Globe
                            ref={globeRef}
                            width={800}
                            height={480}
                            globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
                            bumpImageUrl="//unpkg.com/three-globe/example/img/earth-topology.png"
                        />
                    </Box>
                    {/* Token Section */}
                    <Typography variant="h5" sx={{ mt: 4, mb: 2 }}>Token</Typography>
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={4}>
                            <TrendChart id="priceChart" title="$ Price" trendWindow={trendWindows.price} setTrendWindow={win => setTrendWindows(w => ({ ...w, price: win }))} currentValue={priceCurrent} setCurrentValue={setPriceCurrent} />
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <TrendChart id="volumeChart" title="Volume" trendWindow={trendWindows.volume} setTrendWindow={win => setTrendWindows(w => ({ ...w, volume: win }))} currentValue={volumeCurrent} setCurrentValue={setVolumeCurrent} />
                        </Grid>
                        <Grid item xs={12} md={4}>
                            <TrendChart id="marketCapChart" title="Market Capitalization" trendWindow={trendWindows.marketCap} setTrendWindow={win => setTrendWindows(w => ({ ...w, marketCap: win }))} currentValue={marketCapCurrent} setCurrentValue={setMarketCapCurrent} />
                        </Grid>
                    </Grid>
                </Paper>
            </Container>
        </Box>
    </ThemeProvider>
);
}
