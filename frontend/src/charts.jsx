// charts.js - Reusable chart components for Stats Salad dashboard
// Uses Material-UI, Chart.js, and custom data generators

import React from 'react';
import 'chartjs-adapter-date-fns';
import { Box, Typography, useTheme } from '@mui/material';
import { Chart } from 'chart.js/auto';

// Default plot height for all charts
const plotHeight = 300;

// Format dates specifically for tooltips (more detailed)
const formatTooltipDate = (timestamp, window) => {
  const date = new Date(timestamp);
  if (window === 'day') {
    // More detailed: "Dec 19, 2024 at 14:30"
    return date.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
  } else if (window === 'week') {
    // More detailed: "Dec 19, 2024 at 14:30"
    return date.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
    });
  } else {
    // Month view: "Dec 19, 2024" (no time)
    return date.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
    });
  }
};

// Common tooltip configuration for both chart types
const getTooltipConfig = (isDark, originalTimestamps, window) => ({
  backgroundColor: isDark ? 'rgba(0,0,0,0.8)' : '#fff',
  titleColor: isDark ? '#fff' : '#000',
  bodyColor: isDark ? '#fff' : '#000',
  usePointStyle: true,
  pointStyle: 'circle',
  callbacks: {
    title: function (context) {
      // Use original timestamp for custom formatting
      const index = context[0].dataIndex;
      const timestamp = originalTimestamps[index];
      return formatTooltipDate(timestamp, window);
    },
  },
});

/**
 * TrendChart - Line chart for single series trends
 * Props:
 *   id: string - unique canvas id
 *   title: string - chart title
 *   trendWindow: string - time window ('month', 'week', 'day')
 *   setTrendWindow: function - updates time window
 *   currentValue: number|null - current value to display
 *   setCurrentValue: function - updates current value
 *   unit: string (optional) - unit to display
 *   unitType: 'front'|'below' (optional) - unit display style
 */
export function TrendChart({ id, title, trendWindow, trendData, unit, unitType, isLoading }) {
  const theme = useTheme();
  const isDark = theme.palette.mode === 'dark';
  const chartRef = React.useRef(null);
  const prevTrendData = React.useRef(null);

  // Use lighter green for current values in dark mode
  const valueColor = isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)';
  const headingColor = valueColor;

  // Determine y-axis scale and label formatting
  function getYAxisFormat(maxVal) {
    if (maxVal >= 1e12) return { title: 'Trillions', factor: 1e12, suffix: 'T' };
    if (maxVal >= 1e9) return { title: 'Billions', factor: 1e9, suffix: 'B' };
    if (maxVal >= 1e6) return { title: 'Millions', factor: 1e6, suffix: 'M' };
    if (maxVal >= 1e3) return { title: 'Thousands', factor: 1e3, suffix: 'k' };
    return { title: '', factor: 1, suffix: '' };
  }

  function formatXAxis(ts, window) {
    const date = new Date(ts);
    if (window === 'day') {
      // DD HH:MM (locale-aware)
      return date.toLocaleString(undefined, {
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      });
    } else {
      // DD MMM YYYY (locale-aware)
      return date.toLocaleString(undefined, { day: '2-digit', month: 'short', year: 'numeric' });
    }
  }

  React.useEffect(() => {
    const canvas = document.getElementById(id);
    if (!canvas) return;

    const yMax = trendData.length > 0 ? Math.max(...trendData.map((d) => Math.abs(d.y))) : 0;
    const yFormat = getYAxisFormat(yMax);
    const originalTimestamps = trendData.map((d) => d.x);

    const dataset = {
      label: title,
      data: trendData.map((d) => ({ x: d.x, y: d.y })),
      backgroundColor: 'rgba(83,166,38,0.15)',
      borderColor: 'rgb(83,166,38)',
      borderWidth: 2,
      fill: true,
      pointRadius: 0,
    };

    if (!chartRef.current) {
      chartRef.current = new Chart(canvas, {
        type: 'line',
        data: {
          datasets: [dataset],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          animation: {
            duration: 0,
          },
          plugins: {
            legend: { display: false },
            title: { display: false },
            tooltip: getTooltipConfig(isDark, originalTimestamps, trendWindow),
          },
          interaction: {
            mode: 'index',
            intersect: false,
          },
          scales: {
            x: {
              type: 'time',
              offset: true,
              bounds: 'data',
              time: {
                unit: trendWindow === 'day' ? 'hour' : 'day',
                tooltipFormat: 'yyyy-MM-dd HH:mm',
                displayFormats: {
                  hour: 'MMM d, HH:mm',
                  day: 'MMM d',
                },
              },
              title: { display: false },
              ticks: {
                autoSkip: true,
                maxTicksLimit: 5,
                color: isDark ? 'rgba(255, 255, 255, 0.6)' : 'rgba(0, 0, 0, 0.7)',
              },
              grid: {
                color: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
              },
            },
            y: {
              title: { display: false },
              beginAtZero: true,
              grid: {
                color: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
              },
              ticks: {
                color: isDark ? 'rgba(255, 255, 255, 0.6)' : 'rgba(0, 0, 0, 0.7)',
                callback: function (value) {
                  if (yFormat.factor === 1) return value.toLocaleString();
                  const v = value / yFormat.factor;
                  if (v % 1 === 0) return v + yFormat.suffix;
                  if (Math.abs(v) < 10) return v.toFixed(2).replace(/\.?0+$/, '') + yFormat.suffix;
                  if (Math.abs(v) < 100) return v.toFixed(1).replace(/\.?0+$/, '') + yFormat.suffix;
                  return Math.round(v) + yFormat.suffix;
                },
              },
            },
          },
        },
      });
    } else {
      // Update chart instance
      const chart = chartRef.current;
      chart.data.datasets = [dataset];
      chart.options.scales.x.type = 'time';
      chart.options.scales.x.offset = true;
      chart.options.scales.x.bounds = 'data';
      chart.options.scales.x.time = {
        unit: trendWindow === 'day' ? 'hour' : 'day',
        tooltipFormat: 'yyyy-MM-dd HH:mm',
        displayFormats: {
          hour: 'MMM d, HH:mm',
          day: 'MMM d',
        },
      };
      chart.options.scales.y.ticks.callback = function (value) {
        if (yFormat.factor === 1) return value.toLocaleString();
        const v = value / yFormat.factor;
        if (v % 1 === 0) return v + yFormat.suffix;
        if (Math.abs(v) < 10) return v.toFixed(2).replace(/\.?0+$/, '') + yFormat.suffix;
        if (Math.abs(v) < 100) return v.toFixed(1).replace(/\.?0+$/, '') + yFormat.suffix;
        return Math.round(v) + yFormat.suffix;
      };
      chart.update('none');
    }

    prevTrendData.current = trendData;

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [id, trendData, trendWindow, title]);

  // Update colors when theme changes without reloading chart
  React.useEffect(() => {
    if (chartRef.current) {
      const chart = chartRef.current;
      const isDark = theme.palette.mode === 'dark';
      chart.options.scales.x.ticks.color = isDark
        ? 'rgba(255, 255, 255, 0.6)'
        : 'rgba(0, 0, 0, 0.7)';
      chart.options.scales.x.grid.color = isDark
        ? 'rgba(255, 255, 255, 0.1)'
        : 'rgba(0, 0, 0, 0.1)';
      chart.options.scales.y.grid.color = isDark
        ? 'rgba(255, 255, 255, 0.1)'
        : 'rgba(0, 0, 0, 0.1)';
      chart.options.scales.y.ticks.color = isDark
        ? 'rgba(255, 255, 255, 0.6)'
        : 'rgba(0, 0, 0, 0.7)';
      // Update tooltip config with current theme and timestamps
      const originalTimestamps = trendData.map((d) => d.x);
      chart.options.plugins.tooltip = getTooltipConfig(isDark, originalTimestamps, trendWindow);
      chart.update('none');
    }
  }, [theme.palette.mode, trendData, trendWindow]);

  // Value display logic
  const lastValue = trendData.length > 0 ? trendData[trendData.length - 1].y : null;

  // Time period clarification for current value
  const valuePeriodLabel = {
    day: 'last hour:',
    week: 'last hour:',
    month: 'last day:',
  };

  function formatValue(val, unit) {
    if (val === null || val === undefined) return { value: '--', unit };
    // Special handling for GB
    if (unit === 'GB') {
      if (Math.abs(val) >= 1e12) {
        // EB (exabytes, for completeness)
        let eb = val / 1e12;
        return { value: eb % 1 === 0 ? eb.toFixed(0) : eb.toFixed(eb < 10 ? 2 : 1), unit: 'EB' };
      } else if (Math.abs(val) >= 1e9) {
        // ZB (zettabytes, for completeness)
        let zb = val / 1e9;
        return { value: zb % 1 === 0 ? zb.toFixed(0) : zb.toFixed(zb < 10 ? 2 : 1), unit: 'ZB' };
      } else if (Math.abs(val) >= 1e6) {
        // PB
        let pb = val / 1e6;
        return { value: pb % 1 === 0 ? pb.toFixed(0) : pb.toFixed(pb < 10 ? 2 : 1), unit: 'PB' };
      } else if (Math.abs(val) >= 1e3) {
        // TB
        let tb = val / 1e3;
        return { value: tb % 1 === 0 ? tb.toFixed(0) : tb.toFixed(tb < 10 ? 2 : 1), unit: 'TB' };
      } else {
        return { value: val.toLocaleString(), unit: 'GB' };
      }
    }
    // k/M/B/T formatting for other units
    if (Math.abs(val) < 1e4) {
      return { value: val.toLocaleString(), unit };
    } else if (Math.abs(val) < 1e5) {
      return { value: (val / 1e3).toFixed(1) + 'k', unit };
    } else if (Math.abs(val) < 1e6) {
      return { value: Math.round(val / 1e3) + 'k', unit };
    } else if (Math.abs(val) < 1e9) {
      let m = val / 1e6;
      return { value: m % 1 === 0 ? m.toFixed(0) + 'M' : m.toFixed(m < 10 ? 2 : 1) + 'M', unit };
    } else if (Math.abs(val) < 1e12) {
      let b = val / 1e9;
      return { value: b % 1 === 0 ? b.toFixed(0) + 'B' : b.toFixed(b < 10 ? 2 : 1) + 'B', unit };
    } else {
      let t = val / 1e12;
      return { value: t % 1 === 0 ? t.toFixed(0) + 'T' : t.toFixed(t < 10 ? 2 : 1) + 'T', unit };
    }
  }
  let valueDisplay = '--';
  const { value: formattedValue, unit: formattedUnit } = formatValue(lastValue, unit);
  if (lastValue !== null) {
    if (unitType === 'front' && formattedUnit) {
      valueDisplay = (
        <Box display="flex" alignItems="center">
          <Typography
            variant="body2"
            sx={{ fontSize: '1.2rem', color: valueColor, mr: 0.5, fontWeight: 700 }}
          >
            {formattedUnit}
          </Typography>
          <Typography variant="h4" sx={{ fontWeight: 700, color: valueColor, fontSize: '2.5rem' }}>
            {formattedValue}
          </Typography>
        </Box>
      );
    } else if (unitType === 'below' && formattedUnit) {
      valueDisplay = (
        <Box display="flex" flexDirection="column" alignItems="center">
          <Typography variant="h4" sx={{ fontWeight: 700, color: valueColor, fontSize: '2.5rem' }}>
            {formattedValue}
          </Typography>
          <Typography
            variant="body2"
            sx={{ fontSize: '1.1rem', color: valueColor, mt: 0.5, fontWeight: 700 }}
          >
            {formattedUnit}
          </Typography>
        </Box>
      );
    } else {
      valueDisplay = (
        <Typography variant="h4" sx={{ fontWeight: 700, color: valueColor, fontSize: '2.5rem' }}>
          {formattedValue}
        </Typography>
      );
    }
  }

  // Time window display mapping
  const timeLabels = {
    day: 'over the last day',
    week: 'over the last week',
    month: 'over the last month',
  };

  return (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      sx={{ width: '100%' }}
      className="w-block"
    >
      <Box display="flex" alignItems="center" sx={{ mb: 1, width: '100%' }}>
        <Typography
          variant="h6"
          component="h3"
          sx={{ textAlign: 'left', color: theme.palette.primary.main }}
          className="w-block"
        >
          {title}
        </Typography>
        <Typography variant="body2" sx={{ color: '#aaa', fontSize: '0.95rem', ml: 2 }}>
          {timeLabels[trendWindow] || 'last month'}
        </Typography>
      </Box>
      <Box
        display="flex"
        flexDirection={{ xs: 'column', sm: 'row' }}
        alignItems="center"
        justifyContent="center"
        sx={{ width: '100%', maxWidth: 800, mb: 1, mx: 'auto' }}
        className="w-clearfix"
      >
        <Box sx={{ width: { xs: '100%', sm: 460 }, minWidth: 260 }} className="w-inline-block">
          <canvas
            id={id}
            width="100%"
            height={plotHeight}
            style={{
              width: '100%',
              maxWidth: 460,
              minWidth: 260,
              height: plotHeight,
              display: 'block',
            }}
          ></canvas>
        </Box>
        <Box
          sx={{
            ml: { sm: 1.5, xs: 0.5 },
            mt: { xs: 1, sm: 0 },
            minWidth: 0,
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
          }}
          className="w-inline-block"
        >
          <Typography variant="caption" sx={{ color: '#aaa', fontSize: '0.8rem', mb: 0.5 }}>
            {valuePeriodLabel[trendWindow] || 'last day:'}
          </Typography>
          {valueDisplay}
        </Box>
      </Box>
    </Box>
  );
}

/**
 * StackedChart - Multi-series stacked line chart
 * Props:
 *   id: string - unique canvas id
 *   title: string - chart title
 *   trendWindow: string - time window
 *   setTrendWindow: function - updates time window
 *   labels: array - series labels
 */
export function StackedChart({ id, title, trendWindow, setTrendWindow, labels, chartData, unit }) {
  const theme = useTheme();
  const isDark = theme.palette.mode === 'dark';
  const chartRef = React.useRef(null);
  const [internalChartData, setInternalChartData] = React.useState(null);
  const [currents, setCurrents] = React.useState([]);
  const [isNarrow, setIsNarrow] = React.useState(
    typeof window !== 'undefined' ? window.innerWidth < 1400 : false,
  );

  // Legend colors for stacked chart - 6 shades of green from lightest to darkest (since sorted highest to lowest)
  const legendColors = ['#b2d530', '#9acc35', '#7bb82e', '#53a626', '#3d6b28', '#1f4f22'];

  // Responsive legend position
  React.useEffect(() => {
    function handleResize() {
      setIsNarrow(window.innerWidth < 1400);
    }
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Format x-axis labels consistently
  function formatXAxis(ts, window) {
    const date = new Date(ts);
    if (window === 'day') {
      // DD HH:MM (locale-aware)
      return date.toLocaleString(undefined, {
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      });
    } else {
      // DD MMM YYYY (locale-aware)
      return date.toLocaleString(undefined, { day: '2-digit', month: 'short', year: 'numeric' });
    }
  }

  // Truncate labels to 8 characters with ellipsis, remove RTX/GTX prefix and parenthetical for legend only
  function legendLabel(label) {
    if (!label) return label;
    // Remove RTX/GTX prefix (case-insensitive, with or without space)
    let clean = label.replace(/^(rtx|gtx)\s*/i, '');
    // Extract parenthetical (e.g., (8GB)) if present
    const parenMatch = clean.match(/\([^)]*\)/);
    const paren = parenMatch ? parenMatch[0] : '';
    // Remove parenthetical for truncation, but add it back after
    clean = clean.replace(/\s*\([^)]*\)/g, '').trim();
    const maxLen = 24;
    let result = clean;
    if (clean.length > maxLen) {
      result = clean.substring(0, maxLen) + '...';
    }
    // Add back parenthetical if present
    return paren ? result + ' ' + paren : result;
  }

  // Use provided chartData, but convert to time scale format
  React.useEffect(() => {
    if (chartData && chartData.labels && chartData.datasets) {
      // Convert each dataset's data to [{x, y}] objects for time scale
      const datasets = chartData.datasets.map((ds) => ({
        ...ds,
        data: chartData.labels.map((label, i) => ({ x: label, y: ds.data[i] })),
      }));
      setInternalChartData({ datasets });
      setCurrents(chartData.datasets.map((ds) => ds.data[ds.data.length - 1]));
    } else {
      setInternalChartData(null);
      setCurrents([]);
    }
  }, [chartData]);

  // Determine y-axis scale and label formatting for StackedChart
  function getYAxisFormat(maxVal) {
    if (maxVal >= 1e12) return { title: 'Trillions', factor: 1e12, suffix: 'T' };
    if (maxVal >= 1e9) return { title: 'Billions', factor: 1e9, suffix: 'B' };
    if (maxVal >= 1e6) return { title: 'Millions', factor: 1e6, suffix: 'M' };
    if (maxVal >= 1e3) return { title: 'Thousands', factor: 1e3, suffix: 'k' };
    return { title: '', factor: 1, suffix: '' };
  }
  const yMax = internalChartData
    ? Math.max(...internalChartData.datasets.flatMap((ds) => ds.data).map((d) => Math.abs(d)))
    : 0;
  const yFormat = getYAxisFormat(yMax);

  // Render chart when data changes
  React.useEffect(() => {
    const ctx = document.getElementById(id);
    if (!ctx) return;

    const yMax = internalChartData
      ? Math.max(...internalChartData.datasets.flatMap((ds) => ds.data.map((d) => Math.abs(d.y))))
      : 0;
    const yFormat = getYAxisFormat(yMax);

    if (!chartRef.current) {
      if (
        internalChartData &&
        internalChartData.datasets &&
        internalChartData.datasets.length > 0
      ) {
        const originalTimestamps = chartData.labels;
        chartRef.current = new Chart(ctx, {
          type: 'line',
          data: internalChartData,
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
              duration: 0,
            },
            plugins: {
              legend: { display: false },
              title: { display: false },
              tooltip: getTooltipConfig(isDark, originalTimestamps, trendWindow),
            },
            elements: { point: { radius: 0, hoverRadius: 0, borderWidth: 0 } },
            scales: {
              x: {
                type: 'time',
                offset: true,
                bounds: 'data',
                time: {
                  unit: trendWindow === 'day' ? 'hour' : 'day',
                  tooltipFormat: 'yyyy-MM-dd HH:mm',
                  displayFormats: {
                    hour: 'MMM d, HH:mm',
                    day: 'MMM d',
                  },
                },
                title: { display: false },
                ticks: {
                  autoSkip: true,
                  maxTicksLimit: 5,
                  color: isDark ? 'rgba(255, 255, 255, 0.8)' : 'rgba(0, 0, 0, 0.7)',
                },
                grid: {
                  color: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
                },
              },
              y: {
                stacked: true,
                title: {
                  display: false,
                },
                beginAtZero: true,
                grid: {
                  color: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
                },
                ticks: {
                  color: isDark ? 'rgba(255, 255, 255, 0.8)' : 'rgba(0, 0, 0, 0.7)',
                  callback: function (value) {
                    if (yFormat.factor === 1) return value.toLocaleString();
                    const v = value / yFormat.factor;
                    if (v % 1 === 0) return v + yFormat.suffix;
                    if (Math.abs(v) < 10)
                      return v.toFixed(2).replace(/\.?0+$/, '') + yFormat.suffix;
                    if (Math.abs(v) < 100)
                      return v.toFixed(1).replace(/\.?0+$/, '') + yFormat.suffix;
                    return Math.round(v) + yFormat.suffix;
                  },
                },
              },
            },
            interaction: {
              mode: 'index',
              intersect: false,
            },
          },
        });
      }
    } else {
      const chart = chartRef.current;
      if (internalChartData) {
        chart.data = internalChartData;
        chart.options.scales.x.type = 'time';
        chart.options.scales.x.offset = true;
        chart.options.scales.x.bounds = 'data';
        chart.options.scales.x.time = {
          unit: trendWindow === 'day' ? 'hour' : 'day',
          tooltipFormat: 'yyyy-MM-dd HH:mm',
          displayFormats: {
            hour: 'MMM d, HH:mm',
            day: 'MMM d',
          },
        };
        chart.options.scales.y.ticks.callback = function (value) {
          if (yFormat.factor === 1) return value.toLocaleString();
          const v = value / yFormat.factor;
          if (v % 1 === 0) return v + yFormat.suffix;
          if (Math.abs(v) < 10) return v.toFixed(2).replace(/\.?0+$/, '') + yFormat.suffix;
          if (Math.abs(v) < 100) return v.toFixed(1).replace(/\.?0+$/, '') + yFormat.suffix;
          return Math.round(v) + yFormat.suffix;
        };
        chart.update('none');
      }
    }

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [id, internalChartData, isDark, trendWindow]);

  // Update colors when theme changes without reloading chart
  React.useEffect(() => {
    if (chartRef.current) {
      const chart = chartRef.current;
      const isDark = theme.palette.mode === 'dark';
      chart.options.scales.x.ticks.color = isDark
        ? 'rgba(255, 255, 255, 0.8)'
        : 'rgba(0, 0, 0, 0.7)';
      chart.options.scales.x.grid.color = isDark
        ? 'rgba(255, 255, 255, 0.1)'
        : 'rgba(0, 0, 0, 0.1)';
      chart.options.scales.y.grid.color = isDark
        ? 'rgba(255, 255, 255, 0.1)'
        : 'rgba(0, 0, 0, 0.1)';
      chart.options.scales.y.ticks.color = isDark
        ? 'rgba(255, 255, 255, 0.8)'
        : 'rgba(0, 0, 0, 0.7)';
      // Update tooltip config with current theme and timestamps
      const originalTimestamps = chartData ? chartData.labels : [];
      chart.options.plugins.tooltip = getTooltipConfig(isDark, originalTimestamps, trendWindow);
      chart.update('none');
    }
  }, [theme.palette.mode, chartData, trendWindow]);

  // Time window display mapping
  const timeLabels = {
    day: 'over the last day',
    week: 'over the last week',
    month: 'over the last month',
  };

  return (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="center"
      justifyContent="center"
      sx={{ width: '100%' }}
      className="w-block"
    >
      <Box display="flex" alignItems="center" sx={{ mb: 1, width: '100%' }}>
        <Typography
          variant="h6"
          component="h3"
          sx={{ textAlign: 'left', color: theme.palette.primary.main }}
          className="w-block"
        >
          {title}
        </Typography>
        <Typography variant="body2" sx={{ color: '#aaa', fontSize: '0.95rem', ml: 2 }}>
          {timeLabels[trendWindow] || 'last month'}
        </Typography>
      </Box>
      <Box
        display="flex"
        flexDirection={{ xs: 'column', sm: 'row' }}
        alignItems="center"
        justifyContent="center"
        sx={{ width: '100%', maxWidth: 800, mb: 1, mx: 'auto' }}
        className="w-clearfix"
      >
        <Box sx={{ width: { xs: '100%', sm: 460 }, minWidth: 260 }} className="w-inline-block">
          <canvas
            id={id}
            width="100%"
            height={plotHeight}
            style={{
              width: '100%',
              maxWidth: 460,
              minWidth: 260,
              height: plotHeight,
              display: 'block',
            }}
          ></canvas>
        </Box>
        {/* Legend/value display: right or below chart depending on width */}
        {!isNarrow && (
          <Box sx={{ ml: 1.5, mt: 0, minWidth: 0, flex: 1 }} className="w-inline-block">
            <Typography
              variant="caption"
              sx={{ color: '#aaa', fontSize: '0.8rem', mb: 1, display: 'block' }}
            >
              {trendWindow === 'day' ? 'last hour:' : 'last day:'}
            </Typography>
            {currents.length > 0 && internalChartData && internalChartData.datasets
              ? currents.map((val, idx) => (
                  <Box
                    key={idx}
                    display="flex"
                    alignItems="center"
                    justifyContent="flex-start"
                    sx={{ mb: 1 }}
                    className="w-inline-block"
                  >
                    <Box
                      sx={{
                        width: 12,
                        height: 12,
                        borderRadius: '50%',
                        bgcolor: legendColors[idx % legendColors.length],
                        display: 'inline-block',
                        mr: 1,
                        border: '1px solid #bbb',
                      }}
                    />
                    <Typography
                      variant="body2"
                      component="span"
                      sx={{
                        color: isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)',
                        fontSize: '0.875rem',
                        mr: 1,
                        fontWeight: 400,
                      }}
                    >
                      <span style={{ fontWeight: 700 }}>{val}</span>{' '}
                      {legendLabel(internalChartData.datasets[idx]?.label)}
                    </Typography>
                  </Box>
                ))
              : '--'}
          </Box>
        )}
        {isNarrow && (
          <Box
            sx={{
              width: '100%',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              mt: 2,
            }}
            className="w-inline-block"
          >
            <Typography variant="caption" sx={{ color: '#aaa', fontSize: '0.8rem', mb: 1 }}>
              {trendWindow === 'day' ? 'last hour' : 'last day'}
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center' }}>
              {currents.length > 0 && internalChartData && internalChartData.datasets
                ? currents.map((val, idx) => (
                    <Box
                      key={idx}
                      display="flex"
                      alignItems="center"
                      justifyContent="center"
                      sx={{ mb: 1, mx: 2 }}
                      className="w-inline-block"
                    >
                      <Box
                        sx={{
                          width: 12,
                          height: 12,
                          borderRadius: '50%',
                          bgcolor: legendColors[idx % legendColors.length],
                          display: 'inline-block',
                          mr: 1,
                          border: '1px solid #bbb',
                        }}
                      />
                      <Typography
                        variant="body2"
                        component="span"
                        sx={{
                          color: isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)',
                          fontSize: '0.875rem',
                          mr: 1,
                          fontWeight: 400,
                        }}
                      >
                        <span style={{ fontWeight: 700 }}>{val}</span>{' '}
                        {legendLabel(internalChartData.datasets[idx]?.label)}
                      </Typography>
                    </Box>
                  ))
                : '--'}
            </Box>
          </Box>
        )}
      </Box>
    </Box>
  );
}
