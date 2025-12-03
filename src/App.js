
import React, { useState } from 'react';
import Globe from 'react-globe.gl';
import * as THREE from 'three';
import { Container, Typography, Box, Grid, Paper } from '@mui/material';
import { Chart } from 'chart.js/auto';

function generateRandomData(numPoints = 100) {
  // Generate 100 random points for a trend graph
  // For stacked plot, generate 5 series
  const data = [];
  for (let i = 0; i < numPoints; i++) {
    data.push({
      x: i,
      y: Math.floor(Math.random() * 100)
    });
  }
  return data;
}

function generateStackedData(numPoints = 100, numSeries = 5) {
  const colors = [
    'rgba(255, 99, 132, 0.6)',
    'rgba(54, 162, 235, 0.6)',
    'rgba(255, 206, 86, 0.6)',
    'rgba(75, 192, 192, 0.6)',
    'rgba(153, 102, 255, 0.6)'
  ];
  const borderColors = [
    'rgba(255, 99, 132, 1)',
    'rgba(54, 162, 235, 1)',
    'rgba(255, 206, 86, 1)',
    'rgba(75, 192, 192, 1)',
    'rgba(153, 102, 255, 1)'
  ];
  const labels = Array.from({ length: numPoints }, (_, i) => i);
  const datasets = [];
  for (let s = 0; s < numSeries; s++) {
    const data = Array.from({ length: numPoints }, () => Math.floor(Math.random() * 50 + s * 10));
    datasets.push({
      label: `Series ${s + 1}`,
      data,
      backgroundColor: colors[s],
      borderColor: borderColors[s],
      borderWidth: 1,
      fill: true
    });
  }
  return { labels, datasets };
}

function App() {
  const [trend1Current, setTrend1Current] = useState(null);
  const [stackedCurrents, setStackedCurrents] = useState([]);
  const [trendWindows, setTrendWindows] = useState({
    trendChart1: 'month',
    trendChart2: 'month',
    trendChart3: 'month',
    trendChart4: 'month'
  });

  const globeRef = React.useRef();

  React.useEffect(() => {
    const chartIds = ['trendChart1', 'trendChart2', 'trendChart3', 'trendChart4'];
    const chartInstances = [];
    let trend1LastValue = null;
    let stackedLastValues = [];
    chartIds.forEach((id, idx) => {
      const ctx = document.getElementById(id);
      if (ctx) {
        if (ctx._chartInstance) {
          ctx._chartInstance.destroy();
        }
        let chartInstance;
        // Determine number of points for time window
        let numPoints = 100;
        if (trendWindows[id] === 'month') numPoints = 100;
        else if (trendWindows[id] === 'week') numPoints = 30;
        else if (trendWindows[id] === 'day') numPoints = 7;
        if (id === 'trendChart3') {
          // Stacked plot for chart 3
          const stackedData = generateStackedData(numPoints, 5);
          stackedLastValues = stackedData.datasets.map(ds => ds.data[ds.data.length - 1]);
          chartInstance = new Chart(ctx, {
            type: 'line',
            data: stackedData,
            options: {
              responsive: true,
              plugins: {
                legend: {
                  display: true,
                  position: 'top'
                },
                title: {
                  display: false
                }
              },
              scales: {
                x: {
                  title: {
                    display: true,
                    text: 'Index'
                  }
                },
                y: {
                  stacked: true,
                  title: {
                    display: true,
                    text: 'Value'
                  },
                  beginAtZero: true
                }
              }
            }
          });
        } else {
          const trendData = generateRandomData(numPoints);
          if (id === 'trendChart1') {
            trend1LastValue = trendData[trendData.length - 1]?.y ?? null;
          }
          chartInstance = new Chart(ctx, {
            type: id === 'trendChart4' ? 'bar' : 'line',
            data: {
              labels: trendData.map(d => d.x),
              datasets: [
                {
                  label: id === 'trendChart4' ? 'Random Bars' : 'Random Trend',
                  data: trendData.map(d => d.y),
                  backgroundColor: id === 'trendChart4' ? 'rgba(255, 99, 132, 0.2)' : 'rgba(54, 162, 235, 0.2)',
                  borderColor: id === 'trendChart4' ? 'rgba(255, 99, 132, 1)' : 'rgba(54, 162, 235, 1)',
                  borderWidth: 1,
                  fill: id !== 'trendChart4'
                }
              ]
            },
            options: {
              responsive: true,
              plugins: {
                legend: {
                  display: true,
                  position: 'top'
                },
                title: {
                  display: false
                }
              },
              scales: {
                x: {
                  title: {
                    display: true,
                    text: 'Index'
                  }
                },
                y: {
                  title: {
                    display: true,
                    text: 'Value'
                  },
                  beginAtZero: true
                }
              }
            }
          });
        }
        ctx._chartInstance = chartInstance;
        chartInstances.push(chartInstance);
      }
    });
    setTrend1Current(trend1LastValue);
    setStackedCurrents(stackedLastValues);
    return () => {
      chartInstances.forEach(chart => chart.destroy());
    };
  }, [trendWindows]);

  return (
    <Box sx={{ bgcolor: '#f0f2f5', minHeight: '100vh', py: 4 }}>
      <Container maxWidth="lg" sx={{ bgcolor: 'transparent', px: { xs: 1, sm: 2, md: 4 } }}>
        {/* Top-level Page Paper */}
        <Paper elevation={6} sx={{ p: 4, borderRadius: 6, bgcolor: '#e3e6ee' }}>
          {/* First Level Section */}
          <Paper elevation={4} sx={{ bgcolor: '#1976d2', color: 'white', py: 3, mb: 4, borderRadius: 4, textAlign: 'center' }}>
            <Typography variant="h2" component="h1">
              Stats Salad Dashboard
            </Typography>
            <Box sx={{ mt: 2 }}>
              <Typography variant="body1" sx={{ textAlign: 'center' }}>
                Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed euismod, urna eu tincidunt consectetur, nisi nisl aliquam enim, eget cursus enim urna euismod nisi. Pellentesque habitant morbi tristique senectus et netus et malesuada fames ac turpis egestas. Vestibulum ante ipsum primis in faucibus orci luctus et ultrices posuere cubilia curae; Etiam at risus et justo dignissim congue. Donec congue lacinia dui, a porttitor lectus condimentum laoreet. Nunc eu ullamcorper orci. Quisque eget odio ac lectus vestibulum faucibus eget in metus. In pellentesque faucibus vestibulum. Nulla at nulla justo, eget luctus tortor. Nulla facilisi. Duis aliquet egestas purus in blandit.
              </Typography>
            </Box>
          </Paper>
          {/* Second Level Section: Provider Stats */}
          <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: '#f5f5f5' }}>
            <Typography variant="h4" component="h2" sx={{ textAlign: 'center', mb: 3 }}>
              Provider stats
            </Typography>
            <Grid container spacing={4}>
              <Grid item xs={12} md={6}>
                <Paper elevation={3} sx={{ p: 2, height: 320, maxHeight: 320, borderRadius: 4 }}>
                  <Box display="flex" alignItems="center" justifyContent="center">
                    <Box flex={1}>
                      <Typography variant="h6" component="h3" sx={{ mb: 1, textAlign: 'center' }}>
                        Trend Chart 1
                      </Typography>
                      {/* Time window selection */}
                      <Box sx={{ textAlign: 'center', mb: 1 }}>
                        {['month', 'week', 'day'].map(win => (
                          <button
                            key={win}
                            style={{ margin: '0 4px', padding: '4px 12px', background: trendWindows.trendChart1 === win ? '#1976d2' : '#eee', color: trendWindows.trendChart1 === win ? 'white' : 'black', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                            onClick={() => setTrendWindows(w => ({ ...w, trendChart1: win }))}
                          >
                            Last {win}
                          </button>
                        ))}
                      </Box>
                      <canvas id="trendChart1" width="400" height="250"></canvas>
                    </Box>
                    <Box flex={1} sx={{ textAlign: 'center' }}>
                      <Typography variant="h2" component="div" color="primary">
                        {trend1Current !== null ? trend1Current : '--'}
                      </Typography>
                      <Typography variant="subtitle1" color="textSecondary">
                        Current Value
                      </Typography>
                    </Box>
                  </Box>
                </Paper>
              </Grid>
              <Grid item xs={12} md={6}>
                <Paper elevation={3} sx={{ p: 2, height: 320, maxHeight: 320, borderRadius: 4 }}>
                  <Box sx={{ height: 350, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
                    <Typography variant="h6" component="h3" sx={{ mb: 1, textAlign: 'center' }}>
                      Interactive Globe
                    </Typography>
                    <Globe
                      ref={globeRef}
                      width={400}
                      height={250}
                      globeImageUrl="//unpkg.com/three-globe/example/img/earth-blue-marble.jpg"
                      bumpImageUrl="//unpkg.com/three-globe/example/img/earth-topology.png"
                    />
                  </Box>
                </Paper>
              </Grid>
            </Grid>
          </Paper>
          {/* Second Level Section: Additional Provider Metrics */}
          <Paper elevation={2} sx={{ mb: 4, borderRadius: 4, p: 4, bgcolor: '#f5f5f5' }}>
            <Typography variant="h5" component="h4" sx={{ textAlign: 'center', mb: 3 }}>
              Additional Provider Metrics
            </Typography>
            <Grid container spacing={4}>
              <Grid item xs={12} md={6}>
                <Paper elevation={3} sx={{ p: 2, height: 320, maxHeight: 320, borderRadius: 4 }}>
                  <Box display="flex" alignItems="center" justifyContent="center">
                    <Box flex={1}>
                      <Typography variant="h6" component="h3" sx={{ mb: 1, textAlign: 'center' }}>
                        Trend Chart 3 (Stacked)
                      </Typography>
                      {/* Time window selection */}
                      <Box sx={{ textAlign: 'center', mb: 1 }}>
                        {['month', 'week', 'day'].map(win => (
                          <button
                            key={win}
                            style={{ margin: '0 4px', padding: '4px 12px', background: trendWindows.trendChart3 === win ? '#1976d2' : '#eee', color: trendWindows.trendChart3 === win ? 'white' : 'black', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                            onClick={() => setTrendWindows(w => ({ ...w, trendChart3: win }))}
                          >
                            Last {win}
                          </button>
                        ))}
                      </Box>
                      <canvas id="trendChart3" width="400" height="250"></canvas>
                    </Box>
                    <Box flex={1} sx={{ textAlign: 'center' }}>
                      <Typography variant="h5" component="div" color="primary" sx={{ mb: 1, fontSize: '1.5rem' }}>
                        Current Values
                      </Typography>
                      {stackedCurrents.length > 0 ? (
                        stackedCurrents.map((val, idx) => (
                          <Typography key={idx} variant="body1" component="div" sx={{ color: `rgba(${[255,99,132,54,162,235,255,206,86,75,192,192,153,102,255][idx*3]},${[255,99,132,54,162,235,255,206,86,75,192,192,153,102,255][idx*3+1]},${[255,99,132,54,162,235,255,206,86,75,192,192,153,102,255][idx*3+2]},1)`, fontSize: '1rem' }}>
                            Series {idx + 1}: {val}
                          </Typography>
                        ))
                      ) : '--'}
                    </Box>
                  </Box>
                </Paper>
              </Grid>
              <Grid item xs={12} md={6}>
                <Paper elevation={3} sx={{ p: 2, height: 320, maxHeight: 320, borderRadius: 4 }}>
                  <Box>
                    <Typography variant="h6" component="h3" sx={{ mb: 1, textAlign: 'center' }}>
                      Bar Chart 4
                    </Typography>
                    {/* Time window selection */}
                    <Box sx={{ textAlign: 'center', mb: 1 }}>
                      {['month', 'week', 'day'].map(win => (
                        <button
                          key={win}
                          style={{ margin: '0 4px', padding: '4px 12px', background: trendWindows.trendChart4 === win ? '#1976d2' : '#eee', color: trendWindows.trendChart4 === win ? 'white' : 'black', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
                          onClick={() => setTrendWindows(w => ({ ...w, trendChart4: win }))}
                        >
                          Last {win}
                        </button>
                      ))}
                    </Box>
                    <Box sx={{ mt: 2 }}>
                      <canvas id="trendChart4" width="400" height="250"></canvas>
                    </Box>
                  </Box>
                </Paper>
              </Grid>
            </Grid>
          </Paper>
        </Paper>
      </Container>
    </Box>
  );
  // (removed duplicate state and hooks)
}

export default App;
