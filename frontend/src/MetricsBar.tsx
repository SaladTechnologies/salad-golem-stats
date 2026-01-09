import { useState, useEffect } from 'react';
import { Box, Typography, useTheme, Paper } from '@mui/material';

export interface Metric {
  value: number | null | undefined;
  unit?: string;
  unitType?: 'front' | 'after';
  label: string;
}

export interface MetricsBarProps {
  metrics?: Metric[];
}

// Helper for k/M/B/T formatting
function formatNumber(num: number | null | undefined): string {
  if (num === null || num === undefined || isNaN(num)) return '-';
  const abs = Math.abs(num);
  if (abs >= 1e12) return (num / 1e12).toFixed(2) + 'T';
  if (abs >= 1e9) return (num / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return (num / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return (num / 1e3).toFixed(2) + 'k';
  return num.toLocaleString();
}

// Responsive metrics bar
export default function MetricsBar({ metrics = [] }: MetricsBarProps) {
  const theme = useTheme();
  const [width, setWidth] = useState(window.innerWidth);

  useEffect(() => {
    const handleResize = () => setWidth(window.innerWidth);
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Layout logic (width affects CSS, not JS variable)
  void width; // Used for responsive CSS via sx prop

  // Always use bright green background and navy text in dark mode
  const isDark = theme.palette.mode === 'dark';
  const brightGreen =
    (theme.palette as { lime?: string }).lime ||
    ((theme.palette as { salad?: { lime?: string } }).salad?.lime) ||
    '#B2D530';
  const navy = ((theme.palette as { salad?: { navy?: string } }).salad?.navy) || '#0A2133';
  const bg = isDark ? brightGreen : navy;
  const color = isDark ? navy : brightGreen;

  // Each metric is a flex item, wraps as a unit, label can wrap
  return (
    <Paper
      elevation={3}
      sx={{
        width: '100%',
        px: 2,
        py: 2,
        mb: 3,
        borderRadius: 2,
        background: bg,
        color,
        boxShadow: 3,
        display: 'flex',
        flexDirection: 'row',
        alignItems: 'stretch',
        justifyContent: 'center',
        flexWrap: 'wrap',
        gap: 6,
      }}
    >
      {metrics.map((m, j) => {
        const unitType = m.unitType || (m.unit === '$' ? 'front' : 'after');
        return (
          <Box
            key={j}
            sx={{
              textAlign: 'center',
              minWidth: 'min-content',
              maxWidth: '100%',
              flex: '0 1 auto',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              mb: 1,
              overflow: 'hidden',
            }}
          >
            <Typography
              variant="h2"
              fontWeight={900}
              sx={{
                color,
                lineHeight: 1.05,
                fontSize: { xs: '3.2rem', sm: '3.2rem', md: '4.2rem' },
                whiteSpace: 'nowrap',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
              }}
            >
              {unitType === 'front' && m.unit && (
                <Typography
                  component="span"
                  variant="h2"
                  fontWeight={900}
                  sx={{ color, fontSize: 'inherit', mr: 0.5 }}
                >
                  {m.unit}
                </Typography>
              )}
              {formatNumber(m.value)}
              {unitType === 'after' && m.unit && (
                <Typography
                  component="span"
                  variant="subtitle1"
                  fontWeight={600}
                  sx={{ color, fontSize: { xs: '1.1rem', sm: '1.3rem', md: '1.7rem' }, ml: 0.5 }}
                >
                  {m.unit}
                </Typography>
              )}
            </Typography>
            <Typography
              variant="subtitle1"
              sx={{
                opacity: 0.95,
                fontSize: { xs: '0.95rem', sm: '1.1rem', md: '1.2rem' },
                fontWeight: 500,
                mt: 0.5,
                wordBreak: 'break-word',
                whiteSpace: 'normal',
              }}
            >
              {m.label}
            </Typography>
          </Box>
        );
      })}
    </Paper>
  );
}
