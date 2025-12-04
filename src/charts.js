import React from 'react';
import { Box, Typography } from '@mui/material';
import { Chart } from 'chart.js/auto';
import { generateRandomData, generateStackedData } from './data';

  
const plotHeight = 300;


export function TrendChart({ id, title, trendWindow, setTrendWindow, currentValue, setCurrentValue, unit, unitType }) {
  React.useEffect(() => {
    const ctx = document.getElementById(id);
    if (ctx) {
      if (ctx._chartInstance) ctx._chartInstance.destroy();
      let numPoints = trendWindow === 'month' ? 100 : trendWindow === 'week' ? 30 : 7;
      const trendData = generateRandomData(numPoints);
      setCurrentValue(trendData[trendData.length - 1]?.y ?? null);
      ctx._chartInstance = new Chart(ctx, {
        type: 'line',
        data: {
          labels: trendData.map(d => d.x),
          datasets: [{
            label: title,
            data: trendData.map(d => d.y),
            backgroundColor: 'rgba(54, 162, 235, 0.2)',
            borderColor: 'rgba(54, 162, 235, 1)',
            borderWidth: 1,
            fill: true
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, title: { display: false } },
          scales: { x: { title: { display: true, text: 'Index' } }, y: { title: { display: true, text: 'Value' }, beginAtZero: true } }
        }
      });
    }
    return () => { if (ctx && ctx._chartInstance) ctx._chartInstance.destroy(); };
  }, [id, trendWindow, setCurrentValue, title]);

  // Value display logic
  let valueDisplay = '--';
  if (currentValue !== null) {
    if (unitType === 'front' && unit) {
      valueDisplay = (
        <Box display="flex" alignItems="center">
          <Typography variant="body2" sx={{ fontSize: '1.2rem', color: 'primary.main', mr: 0.5 }}>{unit}</Typography>
          <Typography variant="h4" color="primary" sx={{ fontWeight: 500 }}>{currentValue}</Typography>
        </Box>
      );
    } else if (unitType === 'below' && unit) {
      valueDisplay = (
        <Box display="flex" flexDirection="column" alignItems="center">
          <Typography variant="h4" color="primary" sx={{ fontWeight: 500 }}>{currentValue}</Typography>
          <Typography variant="body2" sx={{ fontSize: '1.1rem', color: 'primary.main', mt: 0.5 }}>{unit}</Typography>
        </Box>
      );
    } else {
      valueDisplay = (
        <Typography variant="h4" color="primary" sx={{ fontWeight: 500 }}>{currentValue}</Typography>
      );
    }
  }

  return (
    <Box display="flex" flexDirection="column" alignItems="center" justifyContent="center" sx={{ width: '100%' }}>
      <Typography variant="h6" component="h3" sx={{ mb: 1, textAlign: 'center' }}>{title}</Typography>
      <Box display="flex" flexDirection={{ xs: 'column', sm: 'row' }} alignItems="center" justifyContent="flex-start" sx={{ width: '100%', maxWidth: 700, mb: 1 }}>
        <Box sx={{ width: { xs: '100%', sm: 600 }, minWidth: 300 }}>
          <canvas id={id} width="100%" height={plotHeight} style={{ width: '100%', maxWidth: 600, minWidth: 300, height: plotHeight, display: 'block' }}></canvas>
        </Box>
        <Box sx={{ ml: { sm: 1, xs: 0 }, mt: { xs: 1, sm: 0 }, minWidth: 0, display: 'flex', flexDirection: 'column', alignItems: unitType === 'below' ? 'center' : 'flex-start' }}>
          {valueDisplay}
        </Box>
      </Box>
      <Box sx={{ textAlign: 'center', mt: 1 }}>
        {globalThis.trendWindows?.map?.(win => (
          <button
            key={win}
            style={{ margin: '0 4px', padding: '4px 12px', background: trendWindow === win ? '#1976d2' : '#eee', color: trendWindow === win ? 'white' : 'black', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
            onClick={() => setTrendWindow(win)}
          >
            Last {win}
          </button>
        )) ?? ['month', 'week', 'day'].map(win => (
          <button
            key={win}
            style={{ margin: '0 4px', padding: '4px 12px', background: trendWindow === win ? '#1976d2' : '#eee', color: trendWindow === win ? 'white' : 'black', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
            onClick={() => setTrendWindow(win)}
          >
            Last {win}
          </button>
        ))}
      </Box>
    </Box>
  );
}


export function StackedChart({ id, title, trendWindow, setTrendWindow, labels }) {
  const [chartData, setChartData] = React.useState(null);
  const [currents, setCurrents] = React.useState([]);
  const [isNarrow, setIsNarrow] = React.useState(typeof window !== 'undefined' ? window.innerWidth < 1400 : false);

  React.useEffect(() => {
    function handleResize() {
      setIsNarrow(window.innerWidth < 1400);
    }
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  React.useEffect(() => {
    let numPoints = trendWindow === 'month' ? 100 : trendWindow === 'week' ? 30 : 7;
    const stackedData = generateStackedData(numPoints, labels.length);
    stackedData.datasets.forEach((ds, i) => ds.label = labels[i]);
    setChartData(stackedData);
    setCurrents(stackedData.datasets.map(ds => ds.data[ds.data.length - 1]));
  }, [trendWindow, labels]);

  React.useEffect(() => {
    const ctx = document.getElementById(id);
    if (ctx && chartData) {
      if (ctx._chartInstance) ctx._chartInstance.destroy();
      ctx._chartInstance = new Chart(ctx, {
        type: 'line',
        data: chartData,
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { display: false }, title: { display: false } },
          scales: { x: { title: { display: true, text: 'Index' } }, y: { stacked: true, title: { display: true, text: 'Value' }, beginAtZero: true } }
        }
      });
    }
    return () => { if (ctx && ctx._chartInstance) ctx._chartInstance.destroy(); };
  }, [id, chartData, title]);

  return (
    <Box display="flex" flexDirection="column" alignItems="center" justifyContent="center" sx={{ width: '100%' }}>
      <Typography variant="h6" component="h3" sx={{ mb: 1, textAlign: 'center' }}>{title}</Typography>
      <Box display="flex"
        flexDirection={isNarrow ? 'column' : 'row'}
        alignItems="center"
        justifyContent="flex-start"
        sx={{ width: '100%', maxWidth: 700, mb: 1 }}>
        <Box sx={{ width: { xs: '100%', sm: 500 }, minWidth: 300 }}>
          <canvas id={id} width="100%" height={plotHeight} style={{ width: '100%', maxWidth: 500, minWidth: 300, height: plotHeight, display: 'block' }}></canvas>
        </Box>
        {!isNarrow && (
          <Box sx={{ ml: 2, mt: 0, minWidth: 0, flex: 1 }}>
            {currents.length > 0 ? (
              currents.map((val, idx) => (
                <Box key={idx} display="flex" alignItems="center" justifyContent="flex-start" sx={{ mb: 1 }}>
                  <Box sx={{ width: 12, height: 12, borderRadius: '50%', bgcolor: [
                    'rgba(255, 99, 132, 1)',
                    'rgba(54, 162, 235, 1)',
                    'rgba(255, 206, 86, 1)',
                    'rgba(75, 192, 192, 1)',
                    'rgba(153, 102, 255, 1)'
                  ][idx % 5], display: 'inline-block', mr: 1, border: '1px solid #bbb' }} />
                  <Typography variant="body2" component="span" sx={{ color: 'text.primary', fontSize: '0.875rem', mr: 1 }}>
                    {labels[idx]}: {val}
                  </Typography>
                </Box>
              ))
            ) : '--'}
          </Box>
        )}
        {isNarrow && (
          <Box sx={{ width: '100%', display: 'flex', flexWrap: 'wrap', justifyContent: 'center', mt: 2 }}>
            {currents.length > 0 ? (
              currents.map((val, idx) => (
                <Box key={idx} display="flex" alignItems="center" justifyContent="center" sx={{ mb: 1, mx: 2 }}>
                  <Box sx={{ width: 12, height: 12, borderRadius: '50%', bgcolor: [
                    'rgba(255, 99, 132, 1)',
                    'rgba(54, 162, 235, 1)',
                    'rgba(255, 206, 86, 1)',
                    'rgba(75, 192, 192, 1)',
                    'rgba(153, 102, 255, 1)'
                  ][idx % 5], display: 'inline-block', mr: 1, border: '1px solid #bbb' }} />
                  <Typography variant="body2" component="span" sx={{ color: 'text.primary', fontSize: '0.875rem', mr: 1 }}>
                    {labels[idx]}: {val}
                  </Typography>
                </Box>
              ))
            ) : '--'}
          </Box>
        )}
      </Box>
      <Box sx={{ textAlign: 'center', mt: 1 }}>
        {['month', 'week', 'day'].map(win => (
          <button
            key={win}
            style={{ margin: '0 4px', padding: '4px 12px', background: trendWindow === win ? '#1976d2' : '#eee', color: trendWindow === win ? 'white' : 'black', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
            onClick={() => setTrendWindow(win)}
          >
            Last {win}
          </button>
        ))}
      </Box>
    </Box>
  );
}