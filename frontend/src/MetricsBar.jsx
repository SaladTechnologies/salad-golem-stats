import React from 'react';
import { Box, Typography, useTheme, Paper } from '@mui/material';

// Helper for k/M/B/T formatting
function formatNumber(num) {
  if (num === null || num === undefined || isNaN(num)) return '-';
  const abs = Math.abs(num);
  if (abs >= 1e12) return (num / 1e12).toFixed(2) + 'T';
  if (abs >= 1e9) return (num / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return (num / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return (num / 1e3).toFixed(2) + 'k';
  return num.toLocaleString();
}

// Responsive metrics bar
export default function MetricsBar({ metrics = [] }) {
  const theme = useTheme();
  const [width, setWidth] = React.useState(window.innerWidth);

  React.useEffect(() => {
    const handleResize = () => setWidth(window.innerWidth);
    window.addEventListener('resize', handleResize);
    return () => window.removeEventListener('resize', handleResize);
  }, []);

  // Layout logic
  let flexDirection = 'row';
  let wrap = false;
  let groupSize = metrics.length;
  if (width < 600) {
    flexDirection = 'column';
    wrap = false;
  } else if (width < 900) {
    flexDirection = 'row';
    wrap = true;
    groupSize = Math.ceil(metrics.length / 2);
  }

  // Always use bright green background and navy text in dark mode
  const isDark = theme.palette.mode === 'dark';
  const brightGreen =
    theme.palette.lime || (theme.palette.salad && theme.palette.salad.lime) || '#B2D530';
  const navy = (theme.palette.salad && theme.palette.salad.navy) || '#0A2133';
  const bg = isDark ? brightGreen : theme.palette.grey[900];
  const color = isDark ? navy : theme.palette.success.main;

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
              minWidth: { xs: 120, sm: 140, md: 160 },
              maxWidth: { xs: 180, sm: 220, md: 260 },
              flex: '1 1 140px',
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              mb: 1,
            }}
          >
            <Typography
              variant="h2"
              fontWeight={900}
              sx={{
                color,
                lineHeight: 1.05,
                fontSize: { xs: '2.7rem', sm: '4rem', md: '5rem' },
                whiteSpace: 'nowrap',
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
                  sx={{ color, fontSize: { xs: '1.2rem', sm: '1.5rem', md: '2rem' }, ml: 0.5 }}
                >
                  {m.unit}
                </Typography>
              )}
            </Typography>
            <Typography
              variant="subtitle1"
              sx={{
                opacity: 0.95,
                fontSize: { xs: '1rem', sm: '1.2rem', md: '1.25rem' },
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
