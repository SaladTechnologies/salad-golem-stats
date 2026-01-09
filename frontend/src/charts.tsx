// charts.ts - Reusable chart components for Stats Salad dashboard
// Uses Material-UI, Chart.js, and custom data generators

import { useRef, useEffect, useState, ReactNode } from 'react';
import 'chartjs-adapter-date-fns';
import { Box, Typography, useTheme } from '@mui/material';
import { Chart, TooltipItem, ChartConfiguration, ChartOptions } from 'chart.js/auto';

// Default plot height for all charts
const plotHeight = 300;

// Consistent axis styling for all charts
const getAxisColors = (isDark: boolean) => ({
  tick: isDark ? 'rgba(255, 255, 255, 0.6)' : 'rgba(0, 0, 0, 0.6)',
  grid: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
});

// Time window display mapping
const timeLabels: Record<string, string> = {
  day: 'over the last day',
  week: 'over the last week',
  two_weeks: 'over the last two weeks',
  month: 'over the last month',
  // API period values
  '6h': 'over the last 6 hours',
  '24h': 'over the last 24 hours',
  '7d': 'over the last 7 days',
  '30d': 'over the last 30 days',
  '90d': 'over the last 90 days',
  total: 'all time',
};

// Check if time window uses hourly granularity (7d or less)
const isHourlyGranularity = (window: string): boolean =>
  ['6h', '24h', '7d', 'day', 'week'].includes(window);

// Format dates specifically for tooltips (more detailed)
const formatTooltipDate = (timestamp: number | string, window: string): string => {
  const date = new Date(timestamp);
  if (isHourlyGranularity(window)) {
    // Hourly views (6h, 24h, 7d, day, week): "Dec 19, 2024 at 14:30 UTC"
    return date.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: 'UTC',
      timeZoneName: 'short',
    });
  } else {
    // Monthly/longer views: "Dec 19, 2024 UTC"
    return date.toLocaleString(undefined, {
      month: 'short',
      day: 'numeric',
      year: 'numeric',
      timeZone: 'UTC',
      timeZoneName: 'short',
    });
  }
};

// Common tooltip configuration for both chart types
const getTooltipConfig = (
  isDark: boolean,
  originalTimestamps: (number | string)[],
  window: string,
) => ({
  backgroundColor: isDark ? 'rgba(0,0,0,0.8)' : '#fff',
  titleColor: isDark ? '#fff' : '#000',
  bodyColor: isDark ? '#fff' : '#000',
  displayColors: false, // Remove color swatch from tooltip
  callbacks: {
    title: function (context: TooltipItem<'line'>[]) {
      // Use original timestamp for custom formatting
      const index = context[0].dataIndex;
      const timestamp = originalTimestamps[index];
      return formatTooltipDate(timestamp, window);
    },
  },
});

// Tooltip config for stacked charts - shows only the hovered series
const getStackedTooltipConfig = (
  isDark: boolean,
  originalTimestamps: (number | string)[],
  window: string,
  unit: string | undefined,
  legendColors: string[],
) => ({
  backgroundColor: isDark ? 'rgba(0,0,0,0.9)' : '#fff',
  titleColor: isDark ? '#fff' : '#000',
  bodyColor: isDark ? '#fff' : '#000',
  borderColor: isDark ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.1)',
  borderWidth: 1,
  padding: 12,
  usePointStyle: true,
  pointStyle: 'circle' as const,
  boxPadding: 6,
  callbacks: {
    title: function (context: TooltipItem<'line'>[]) {
      const index = context[0].dataIndex;
      const timestamp = originalTimestamps[index];
      return formatTooltipDate(timestamp, window);
    },
    label: function (context: TooltipItem<'line'>) {
      const label = context.dataset.label || '';
      const value = context.parsed.y ?? 0;
      const formattedValue =
        unit === 'nodes' || unit === 'count'
          ? Math.round(value).toLocaleString()
          : value.toLocaleString(undefined, { maximumFractionDigits: 1 });
      return ` ${label}: ${formattedValue} ${unit || ''}`;
    },
    labelColor: function (context: TooltipItem<'line'>) {
      const color = legendColors[context.datasetIndex % legendColors.length];
      return {
        borderColor: color,
        backgroundColor: color,
        borderWidth: 2,
        borderRadius: 2,
      };
    },
  },
});

// Interaction config for stacked charts - show nearest single dataset
const stackedInteractionConfig = {
  mode: 'nearest' as const,
  intersect: false,
};

export interface TrendDataPoint {
  x: number | string;
  y: number;
}

export interface TrendChartProps {
  id: string;
  title: string;
  description?: string;
  trendWindow: string;
  trendData: TrendDataPoint[];
  unit?: string;
  unitType?: 'front' | 'below';
  isLoading?: boolean; // Currently unused but kept for API consistency
}

interface YAxisFormat {
  title: string;
  factor: number;
  suffix: string;
}

/**
 * TrendChart - Line chart for single series trends
 */
export function TrendChart({
  id,
  title,
  description,
  trendWindow,
  trendData,
  unit,
  unitType,
  isLoading: _isLoading,
}: TrendChartProps) {
  void _isLoading; // Kept for API consistency
  const theme = useTheme();
  const isDark = theme.palette.mode === 'dark';
  const chartRef = useRef<Chart | null>(null);
  const prevTrendData = useRef<TrendDataPoint[] | null>(null);

  // Use lighter green for current values in dark mode
  const valueColor = isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)';

  // Determine y-axis scale and label formatting
  function getYAxisFormat(maxVal: number): YAxisFormat {
    if (maxVal >= 1e12) return { title: 'Trillions', factor: 1e12, suffix: 'T' };
    if (maxVal >= 1e9) return { title: 'Billions', factor: 1e9, suffix: 'B' };
    if (maxVal >= 1e6) return { title: 'Millions', factor: 1e6, suffix: 'M' };
    if (maxVal >= 1e3) return { title: 'Thousands', factor: 1e3, suffix: 'k' };
    return { title: '', factor: 1, suffix: '' };
  }

  useEffect(() => {
    const canvas = document.getElementById(id) as HTMLCanvasElement | null;
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
                unit: isHourlyGranularity(trendWindow) ? 'hour' : 'day',
                displayFormats: {
                  hour: 'MMM d, HH:mm',
                  day: 'MMM d',
                },
              },
              title: { display: false },
              ticks: {
                autoSkip: true,
                maxTicksLimit: 5,
                color: getAxisColors(isDark).tick,
              },
              grid: {
                color: getAxisColors(isDark).grid,
              },
            },
            y: {
              title: { display: false },
              beginAtZero: true,
              grid: {
                color: getAxisColors(isDark).grid,
              },
              ticks: {
                color: getAxisColors(isDark).tick,
                callback: function (value: string | number) {
                  const numValue = typeof value === 'string' ? parseFloat(value) : value;
                  if (yFormat.factor === 1) return numValue.toLocaleString();
                  const v = numValue / yFormat.factor;
                  if (v % 1 === 0) return v + yFormat.suffix;
                  if (Math.abs(v) < 10) return v.toFixed(2).replace(/\.?0+$/, '') + yFormat.suffix;
                  if (Math.abs(v) < 100) return v.toFixed(1).replace(/\.?0+$/, '') + yFormat.suffix;
                  return Math.round(v) + yFormat.suffix;
                },
              },
            },
          },
        } as ChartOptions<'line'>,
      } as ChartConfiguration<'line'>);
    } else {
      // Update chart instance
      const chart = chartRef.current;
      chart.data.datasets = [dataset];
      const options = chart.options as ChartOptions<'line'>;
      if (options.scales?.x) {
        (options.scales.x as { type?: string; offset?: boolean; bounds?: string; time?: { unit?: string; displayFormats?: Record<string, string> } }).type = 'time';
        (options.scales.x as { offset?: boolean }).offset = true;
        (options.scales.x as { bounds?: string }).bounds = 'data';
        (options.scales.x as { time?: { unit?: string; displayFormats?: Record<string, string> } }).time = {
          unit: isHourlyGranularity(trendWindow) ? 'hour' : 'day',
          displayFormats: {
            hour: 'MMM d, HH:mm',
            day: 'MMM d',
          },
        };
      }
      if (options.scales?.y?.ticks) {
        options.scales.y.ticks.callback = function (value: string | number) {
          const numValue = typeof value === 'string' ? parseFloat(value) : value;
          // Show one decimal for 'k' scale unless unit is 'count'
          if (yFormat.factor === 1) return numValue.toLocaleString();
          const v = numValue / yFormat.factor;
          if (unit === 'count') {
            // For count, always show integer
            return Math.round(v) + yFormat.suffix;
          }
          if (yFormat.suffix === 'k') {
            // For k, always show at most 1 decimal
            return v % 1 === 0 ? v + yFormat.suffix : v.toFixed(1).replace(/\.0$/, '') + yFormat.suffix;
          }
          if (Math.abs(v) < 10) return v.toFixed(2).replace(/\.?0+$/, '') + yFormat.suffix;
          if (Math.abs(v) < 100) return v.toFixed(1).replace(/\.?0+$/, '') + yFormat.suffix;
          return Math.round(v) + yFormat.suffix;
        };
      }
      chart.update('none');
    }

    prevTrendData.current = trendData;

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [id, trendData, trendWindow, title, isDark, unit]);

  // Update colors when theme changes without reloading chart
  useEffect(() => {
    if (chartRef.current) {
      const chart = chartRef.current;
      const isDarkMode = theme.palette.mode === 'dark';
      const options = chart.options as ChartOptions<'line'>;
      if (options.scales?.x?.ticks) {
        options.scales.x.ticks.color = getAxisColors(isDarkMode).tick;
      }
      if (options.scales?.x?.grid) {
        options.scales.x.grid.color = getAxisColors(isDarkMode).grid;
      }
      if (options.scales?.y?.grid) {
        options.scales.y.grid.color = getAxisColors(isDarkMode).grid;
      }
      if (options.scales?.y?.ticks) {
        options.scales.y.ticks.color = getAxisColors(isDarkMode).tick;
      }
      // Update tooltip config with current theme and timestamps
      const originalTimestamps = trendData.map((d) => d.x);
      if (options.plugins?.tooltip) {
        Object.assign(options.plugins.tooltip, getTooltipConfig(isDarkMode, originalTimestamps, trendWindow));
      }
      chart.update('none');
    }
  }, [theme.palette.mode, trendData, trendWindow]);

  // Value display logic
  const lastValue = trendData.length > 0 ? trendData[trendData.length - 1].y : null;

  function formatValue(val: number | null, unit: string | undefined): { value: string; unit: string | undefined } {
    if (val === null || val === undefined) return { value: '--', unit };
    // If unit is 'count', always show as integer, no decimals or suffixes
    if (unit === 'count' || unit === 'nodes') {
      return { value: Math.round(val).toLocaleString(), unit };
    }
    // Special handling for GB
    if (unit === 'GB') {
      if (Math.abs(val) >= 1e12) {
        // EB (exabytes, for completeness)
        const eb = val / 1e12;
        return { value: eb % 1 === 0 ? eb.toFixed(0) : eb.toFixed(eb < 10 ? 2 : 1), unit: 'EB' };
      } else if (Math.abs(val) >= 1e9) {
        // ZB (zettabytes, for completeness)
        const zb = val / 1e9;
        return { value: zb % 1 === 0 ? zb.toFixed(0) : zb.toFixed(zb < 10 ? 2 : 1), unit: 'ZB' };
      } else if (Math.abs(val) >= 1e6) {
        // PB
        const pb = val / 1e6;
        return { value: pb % 1 === 0 ? pb.toFixed(0) : pb.toFixed(pb < 10 ? 2 : 1), unit: 'PB' };
      } else if (Math.abs(val) >= 1e3) {
        // TB
        const tb = val / 1e3;
        return { value: tb % 1 === 0 ? tb.toFixed(0) : tb.toFixed(tb < 10 ? 2 : 1), unit: 'TB' };
      } else {
        return { value: val.toLocaleString(), unit: 'GB' };
      }
    }
    // k/M/B/T formatting for other units
    if (Math.abs(val) < 1e3) {
      return { value: val.toLocaleString(), unit };
    } else if (Math.abs(val) < 1e4) {
      return { value: Number(val.toFixed(1)).toLocaleString(), unit };
    } else if (Math.abs(val) < 1e5) {
      return { value: (val / 1e3).toFixed(1) + 'k', unit };
    } else if (Math.abs(val) < 1e6) {
      return { value: Math.round(val / 1e3) + 'k', unit };
    } else if (Math.abs(val) < 1e9) {
      const m = val / 1e6;
      return { value: m % 1 === 0 ? m.toFixed(0) + 'M' : m.toFixed(m < 10 ? 2 : 1) + 'M', unit };
    } else if (Math.abs(val) < 1e12) {
      const b = val / 1e9;
      return { value: b % 1 === 0 ? b.toFixed(0) + 'B' : b.toFixed(b < 10 ? 2 : 1) + 'B', unit };
    } else {
      const t = val / 1e12;
      return { value: t % 1 === 0 ? t.toFixed(0) + 'T' : t.toFixed(t < 10 ? 2 : 1) + 'T', unit };
    }
  }

  let valueDisplay: ReactNode = '--';
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

  return (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="flex-start"
      justifyContent="left"
      sx={{ width: '100%' }}
      className="w-block"
    >
      <Box display="flex" alignItems="center" sx={{ mb: 0, width: '100%' }}>
        <Typography
          variant="h6"
          component="h3"
          sx={{ textAlign: 'left', color: theme.palette.primary.main }}
          className="w-block"
        >
          {title}
        </Typography>
        <Typography
          variant="body2"
          sx={{ color: getAxisColors(isDark).tick, fontSize: '0.95rem', ml: 2 }}
        >
          {timeLabels[trendWindow] || 'last month'}
        </Typography>
      </Box>
      {description && (
        <Typography
          variant="body2"
          sx={{
            color: getAxisColors(isDark).tick,
            fontSize: '0.95rem',
            mb: 2,
            textAlign: 'left',
            lineHeight: 1.4,
          }}
        >
          {description}
        </Typography>
      )}
      <Box
        display="flex"
        flexDirection={{ xs: 'column', sm: 'row' }}
        alignItems="center"
        justifyContent="flex-start"
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
            {isHourlyGranularity(trendWindow) ? 'last hour:' : 'last day:'}
          </Typography>
          {valueDisplay}
        </Box>
      </Box>
    </Box>
  );
}

export interface ChartDataset {
  label: string;
  data: number[];
  backgroundColor?: string;
  borderColor?: string;
  borderWidth?: number;
  fill?: boolean;
}

export interface ChartData {
  labels: (number | string)[];
  datasets: ChartDataset[];
}

export interface StackedChartProps {
  id: string;
  title: string;
  description?: string;
  trendWindow: string;
  setTrendWindow?: (window: string) => void;
  labels?: (number | string)[];
  chartData?: ChartData | null;
  unit?: string;
}

interface InternalDataset {
  label: string;
  data: { x: number | string; y: number }[];
  backgroundColor: string;
  borderColor: string;
  borderWidth: number;
  fill: boolean;
}

interface InternalChartData {
  datasets: InternalDataset[];
}

/**
 * StackedChart - Multi-series stacked line chart
 */
export function StackedChart({
  id,
  title,
  description,
  trendWindow,
  chartData,
  unit,
}: StackedChartProps) {
  const theme = useTheme();
  const isDark = theme.palette.mode === 'dark';
  const chartRef = useRef<Chart | null>(null);
  const [internalChartData, setInternalChartData] = useState<InternalChartData | null>(null);
  const [currents, setCurrents] = useState<number[]>([]);
  const [isNarrow, setIsNarrow] = useState(
    typeof window !== 'undefined' ? window.innerWidth < 1400 : false,
  );
  const [legendBelow, setLegendBelow] = useState(
    typeof window !== 'undefined' ? window.innerWidth < 1100 : false,
  );

  // Legend colors for stacked chart - 6 shades of green from lightest to darkest (since sorted highest to lowest)
  const legendColors = ['#b2d530', '#9acc35', '#7bb82e', '#53a626', '#3d6b28', '#1f4f22'];

  // Responsive legend position and chart width
  useEffect(() => {
    function handleResize() {
      setIsNarrow(window.innerWidth < 1400);
      setLegendBelow(window.innerWidth < 1100);
    }
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Truncate labels to 8 characters with ellipsis, remove RTX/GTX prefix and parenthetical for legend only
  function legendLabel(label: string | undefined): string {
    if (!label) return label || '';
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

  // Use provided chartData, but convert to time scale format and apply colors
  useEffect(() => {
    if (chartData && chartData.labels && chartData.datasets && chartData.datasets.length > 0) {
      // Convert each dataset's data to [{x, y}] objects for time scale, and apply legend colors
      const datasets: InternalDataset[] = chartData.datasets.map((ds, idx) => ({
        ...ds,
        data: chartData.labels.map((label, i) => ({ x: label, y: ds.data[i] })),
        backgroundColor: legendColors[idx % legendColors.length] + '80', // 50% opacity for fill
        borderColor: legendColors[idx % legendColors.length],
        borderWidth: 2,
        fill: true,
      }));
      setInternalChartData({ datasets });
      setCurrents(chartData.datasets.map((ds) => ds.data[ds.data.length - 1]));
    } else {
      // Empty state - create empty chart data structure
      setInternalChartData({ datasets: [] });
      setCurrents([]);
    }
  }, [chartData]);

  // Determine y-axis scale and label formatting for StackedChart
  function getYAxisFormat(maxVal: number): YAxisFormat {
    if (maxVal >= 1e12) return { title: 'Trillions', factor: 1e12, suffix: 'T' };
    if (maxVal >= 1e9) return { title: 'Billions', factor: 1e9, suffix: 'B' };
    if (maxVal >= 1e6) return { title: 'Millions', factor: 1e6, suffix: 'M' };
    if (maxVal >= 1e3) return { title: 'Thousands', factor: 1e3, suffix: 'k' };
    return { title: '', factor: 1, suffix: '' };
  }

  // Render chart when data changes
  useEffect(() => {
    const ctx = document.getElementById(id) as HTMLCanvasElement | null;
    if (!ctx) return;

    const localYMax =
      internalChartData && internalChartData.datasets.length > 0
        ? Math.max(
            ...internalChartData.datasets[0].data.map((_, timeIndex) =>
              // Sum all series values at this time point
              internalChartData.datasets.reduce(
                (sum, ds) => sum + Math.abs(ds.data[timeIndex]?.y || 0),
                0,
              ),
            ),
          )
        : 0;
    const localYFormat = getYAxisFormat(localYMax);

    if (!chartRef.current) {
      // Create chart even with empty data to show empty state
      if (internalChartData) {
        const originalTimestamps = chartData?.labels || [];
        chartRef.current = new Chart(ctx, {
          type: 'line',
          data: internalChartData as { datasets: { label: string; data: { x: number | string; y: number }[]; backgroundColor: string; borderColor: string; borderWidth: number; fill: boolean }[] },
          options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: {
              duration: 0,
            },
            plugins: {
              legend: { display: false },
              title: { display: false },
              tooltip: getStackedTooltipConfig(
                isDark,
                originalTimestamps,
                trendWindow,
                unit,
                legendColors,
              ),
            },
            elements: { point: { radius: 0, hoverRadius: 4, borderWidth: 2 } },
            scales: {
              x: {
                type: 'time',
                offset: true,
                bounds: 'data',
                time: {
                  unit: isHourlyGranularity(trendWindow) ? 'hour' : 'day',
                  displayFormats: {
                    hour: 'MMM d, HH:mm',
                    day: 'MMM d',
                  },
                },
                title: { display: false },
                ticks: {
                  autoSkip: true,
                  maxTicksLimit: 5,
                  color: getAxisColors(isDark).tick,
                },
                grid: {
                  color: getAxisColors(isDark).grid,
                },
              },
              y: {
                stacked: true,
                title: {
                  display: false,
                },
                beginAtZero: true,
                grid: {
                  color: getAxisColors(isDark).grid,
                },
                ticks: {
                  color: getAxisColors(isDark).tick,
                  callback: function (value: string | number) {
                    const numValue = typeof value === 'string' ? parseFloat(value) : value;
                    // Show one decimal for 'k' scale unless unit is 'count'
                    if (localYFormat.factor === 1) return numValue.toLocaleString();
                    const v = numValue / localYFormat.factor;
                    if (unit === 'count') {
                      // For count, always show integer
                      return Math.round(v) + localYFormat.suffix;
                    }
                    if (localYFormat.suffix === 'k') {
                      // For k, show one decimal if not integer
                      return v % 1 === 0 ? v + localYFormat.suffix : v.toFixed(1) + localYFormat.suffix;
                    }
                    if (Math.abs(v) < 10)
                      return v.toFixed(2).replace(/\.?0+$/, '') + localYFormat.suffix;
                    if (Math.abs(v) < 100)
                      return v.toFixed(1).replace(/\.?0+$/, '') + localYFormat.suffix;
                    return Math.round(v) + localYFormat.suffix;
                  },
                },
              },
            },
            interaction: stackedInteractionConfig,
          } as ChartOptions<'line'>,
        } as ChartConfiguration<'line'>);
      }
    } else {
      const chart = chartRef.current;
      if (internalChartData) {
        chart.data = internalChartData as { datasets: { label: string; data: { x: number | string; y: number }[]; backgroundColor: string; borderColor: string; borderWidth: number; fill: boolean }[] };
        const options = chart.options as ChartOptions<'line'>;
        if (options.scales?.x) {
          (options.scales.x as { type?: string }).type = 'time';
          (options.scales.x as { offset?: boolean }).offset = true;
          (options.scales.x as { bounds?: string }).bounds = 'data';
          (options.scales.x as { time?: { unit?: string; displayFormats?: Record<string, string> } }).time = {
            unit: isHourlyGranularity(trendWindow) ? 'hour' : 'day',
            displayFormats: {
              hour: 'MMM d, HH:mm',
              day: 'MMM d',
            },
          };
        }
        if (options.scales?.y?.ticks) {
          options.scales.y.ticks.callback = function (value: string | number) {
            const numValue = typeof value === 'string' ? parseFloat(value) : value;
            if (localYFormat.factor === 1) return numValue.toLocaleString();
            const v = numValue / localYFormat.factor;
            if (v % 1 === 0) {
              // Add commas for large whole numbers
              return v >= 1000 ? v.toLocaleString() + localYFormat.suffix : v + localYFormat.suffix;
            }
            if (Math.abs(v) < 10) return v.toFixed(2).replace(/\.?0+$/, '') + localYFormat.suffix;
            if (Math.abs(v) < 100) return v.toFixed(1).replace(/\.?0+$/, '') + localYFormat.suffix;
            // Add commas for large rounded numbers
            const rounded = Math.round(v);
            return rounded >= 1000
              ? rounded.toLocaleString() + localYFormat.suffix
              : rounded + localYFormat.suffix;
          };
        }
        chart.update('none');
      }
    }

    return () => {
      if (chartRef.current) {
        chartRef.current.destroy();
        chartRef.current = null;
      }
    };
  }, [id, internalChartData, isDark, trendWindow, chartData, unit]);

  // Update colors when theme changes without reloading chart
  useEffect(() => {
    if (chartRef.current) {
      const chart = chartRef.current;
      const isDarkMode = theme.palette.mode === 'dark';
      const options = chart.options as ChartOptions<'line'>;
      if (options.scales?.x?.ticks) {
        options.scales.x.ticks.color = getAxisColors(isDarkMode).tick;
      }
      if (options.scales?.x?.grid) {
        options.scales.x.grid.color = getAxisColors(isDarkMode).grid;
      }
      if (options.scales?.y?.grid) {
        options.scales.y.grid.color = getAxisColors(isDarkMode).grid;
      }
      if (options.scales?.y?.ticks) {
        options.scales.y.ticks.color = getAxisColors(isDarkMode).tick;
      }
      // Update tooltip config with current theme and timestamps
      const originalTimestamps = chartData ? chartData.labels : [];
      if (options.plugins?.tooltip) {
        Object.assign(
          options.plugins.tooltip,
          getStackedTooltipConfig(isDarkMode, originalTimestamps, trendWindow, unit, legendColors),
        );
      }
      chart.update('none');
    }
  }, [theme.palette.mode, chartData, trendWindow, unit]);

  // Helper to format legend values
  const formatLegendValue = (val: number, scale: { factor: number; suffix: string }): string => {
    if (unit === 'count' || unit === 'nodes') {
      // Always show as integer, no decimals or suffixes, even if scaled
      return Math.round(val).toLocaleString();
    }
    if (scale.factor === 1) {
      // Consistent 1 decimal for all non-count values, keep trailing zeros
      return val.toLocaleString(undefined, {
        minimumFractionDigits: 1,
        maximumFractionDigits: 1,
      });
    } else {
      const scaledVal = val / scale.factor;
      // Consistent 1 decimal for all scaled values
      return `${scaledVal.toLocaleString(undefined, { minimumFractionDigits: 1, maximumFractionDigits: 1 })}${scale.suffix}`;
    }
  };

  // Calculate scale based on majority voting
  const getScale = (values: number[]) => {
    const scales = values.map((val) =>
      val >= 1e9 ? 'B' : val >= 1e6 ? 'M' : val >= 1000 ? 'k' : 'raw',
    );
    const scaleCounts: Record<string, number> = { raw: 0, k: 0, M: 0, B: 0 };
    scales.forEach((s) => scaleCounts[s]++);

    // Use the scale that the majority of values would use
    const majorityScale = Object.keys(scaleCounts).reduce((a, b) =>
      scaleCounts[a] > scaleCounts[b] ? a : b,
    );

    return majorityScale === 'B'
      ? { factor: 1e9, suffix: 'B' }
      : majorityScale === 'M'
        ? { factor: 1e6, suffix: 'M' }
        : majorityScale === 'k'
          ? { factor: 1000, suffix: 'k' }
          : { factor: 1, suffix: '' };
  };

  return (
    <Box
      display="flex"
      flexDirection="column"
      alignItems="flex-start"
      justifyContent="left"
      sx={{ width: '100%' }}
      className="w-block"
    >
      <Box display="flex" alignItems="center" sx={{ mb: 0, width: '100%' }}>
        <Typography
          variant="h6"
          component="h3"
          sx={{ textAlign: 'left', color: theme.palette.primary.main }}
          className="w-block"
        >
          {title}
        </Typography>
        <Typography
          variant="body2"
          sx={{ color: getAxisColors(isDark).tick, fontSize: '0.95rem', ml: 2 }}
        >
          {timeLabels[trendWindow] || 'last month'}
        </Typography>
      </Box>
      {description && (
        <Typography
          variant="body2"
          sx={{
            color: getAxisColors(isDark).tick,
            fontSize: '0.95rem',
            mb: 1,
            textAlign: 'left',
            lineHeight: 1.4,
          }}
        >
          {description}
        </Typography>
      )}
      <Box
        display="flex"
        flexDirection={legendBelow ? 'column' : 'row'}
        alignItems="center"
        justifyContent="flex-start"
        sx={{ width: '100%', maxWidth: 800, mb: 1, mx: 'auto' }}
        className="w-clearfix"
      >
        <Box
          sx={{
            width: isNarrow ? { xs: '100%', sm: 360 } : { xs: '100%', sm: 460 },
            minWidth: 260,
          }}
          className="w-inline-block"
        >
          <canvas
            id={id}
            width="100%"
            height={plotHeight}
            style={{
              width: '100%',
              maxWidth: isNarrow ? 360 : 460,
              minWidth: 260,
              height: plotHeight,
              display: 'block',
            }}
          ></canvas>
        </Box>
        {/* Legend/value display: right or below chart depending on width */}
        {!legendBelow && (
          <Box sx={{ ml: 1.5, mt: 0, minWidth: 0, flex: 1 }} className="w-inline-block">
            <Typography
              variant="caption"
              sx={{ color: '#aaa', fontSize: '0.8rem', mb: 1, display: 'block' }}
            >
              {isHourlyGranularity(trendWindow) ? 'last hour:' : 'last day:'}
            </Typography>
            {currents.length > 0 && internalChartData && internalChartData.datasets
              ? (() => {
                  const scale = getScale(currents);
                  return currents.map((val, idx) => (
                    <Box
                      key={idx}
                      sx={{
                        mb: 1,
                        display: 'grid',
                        gridTemplateColumns: '12px auto auto 1fr',
                        gap: 1,
                        alignItems: 'center',
                      }}
                    >
                      <Box
                        sx={{
                          width: 12,
                          height: 12,
                          borderRadius: '50%',
                          bgcolor: legendColors[idx % legendColors.length],
                          border: '1px solid #bbb',
                        }}
                      />
                      <Typography
                        variant="body2"
                        component="span"
                        sx={{
                          color: isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)',
                          fontSize: '0.875rem',
                          fontWeight: 700,
                          textAlign: 'right',
                          minWidth: '4.5em',
                        }}
                      >
                        {formatLegendValue(val, scale)}
                      </Typography>
                      <Typography
                        variant="body2"
                        component="span"
                        sx={{
                          color: isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)',
                          fontSize: '0.875rem',
                          fontWeight: 400,
                          overflow: 'hidden',
                          textOverflow: 'ellipsis',
                          whiteSpace: 'nowrap',
                          maxWidth: '120px',
                        }}
                      >
                        {legendLabel(internalChartData.datasets[idx]?.label)}
                      </Typography>
                    </Box>
                  ));
                })()
              : '--'}
          </Box>
        )}
        {legendBelow && (
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
              {isHourlyGranularity(trendWindow) ? 'last hour' : 'last day'}
            </Typography>
            <Box sx={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'center' }}>
              {currents.length > 0 && internalChartData && internalChartData.datasets
                ? (() => {
                    const scale = getScale(currents);
                    return currents.map((val, idx) => (
                      <Box
                        key={idx}
                        sx={{
                          mb: 1,
                          mx: 2,
                          display: 'grid',
                          gridTemplateColumns: '12px auto auto',
                          gap: 1,
                          alignItems: 'center',
                        }}
                      >
                        <Box
                          sx={{
                            width: 12,
                            height: 12,
                            borderRadius: '50%',
                            bgcolor: legendColors[idx % legendColors.length],
                            border: '1px solid #bbb',
                          }}
                        />
                        <Typography
                          variant="body2"
                          component="span"
                          sx={{
                            color: isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)',
                            fontSize: '0.875rem',
                            fontWeight: 700,
                            textAlign: 'right',
                            minWidth: '4.5em',
                          }}
                        >
                          {formatLegendValue(val, scale)}
                        </Typography>
                        <Typography
                          variant="body2"
                          component="span"
                          sx={{
                            color: isDark ? 'rgb(178,213,48)' : 'rgb(31, 79, 34)',
                            fontSize: '0.875rem',
                            fontWeight: 400,
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                            whiteSpace: 'nowrap',
                            maxWidth: '100px',
                          }}
                        >
                          {legendLabel(internalChartData.datasets[idx]?.label)}
                        </Typography>
                      </Box>
                    ));
                  })()
                : '--'}
            </Box>
          </Box>
        )}
      </Box>
    </Box>
  );
}
