import React from 'react';
import { useReactTable, getCoreRowModel, flexRender } from '@tanstack/react-table';
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
  Divider,
} from '@mui/material';

import { Box, Button, Select, MenuItem } from '@mui/material';

export default function TransactionsTable({ data }) {
  // Pagination state
  const [page, setPage] = React.useState(0);
  const [pageSize, setPageSize] = React.useState(10);
  const totalRows = data ? data.length : 0;
  const pageCount = Math.ceil(totalRows / pageSize);
  const pagedData = React.useMemo(() => {
    if (!data) return [];
    const start = page * pageSize;
    return data.slice(start, start + pageSize);
  }, [data, page, pageSize]);

  const columns = React.useMemo(
    () => [
      {
        accessorKey: 'ts',
        header: 'Timestamp (UTC)',
        cell: (info) => (info.getValue() ? String(info.getValue()).replace('T', ' ') : ''),
      },
      {
        accessorKey: 'provider_wallet',
        header: 'Provider Wallet',
        cell: (info) => {
          const v = info.getValue();
          return v ? String(v).slice(0, 8) + '...' : '';
        },
      },
      {
        accessorKey: 'requester_wallet',
        header: 'Requester Wallet',
        cell: (info) => {
          const v = info.getValue();
          return v ? String(v).slice(0, 8) + '...' : '';
        },
      },
      {
        accessorKey: 'tx',
        header: 'Transaction Hash',
        cell: (info) => {
          const v = info.getValue();
          if (!v) return '';
          const short = String(v).slice(0, 8) + '...';
          return (
            <a
              href={`https://polygonscan.com/tx/${v}`}
              target="_blank"
              rel="noopener noreferrer"
              style={{ color: '#1976d2', textDecoration: 'underline' }}
            >
              {short}
            </a>
          );
        },
      },
      //{ accessorKey: 'gpu', header: 'GPU' },
      // {
      //   accessorKey: 'ram',
      //   header: 'RAM (GB)',
      //   cell: (info) => {
      //     const v = info.getValue();
      //     return v ? Math.round(Number(v) / 1024) : '';
      //   },
      // },
      //{ accessorKey: 'vcpus', header: 'vCPUs' },
      //{ accessorKey: 'duration', header: 'Duration' },
      { accessorKey: 'invoiced_glm', header: 'GLM' },
      {
        accessorKey: 'invoiced_dollar',
        header: 'USD',
        cell: (info) => {
          const v = info.getValue();
          return v !== undefined && v !== null ? `$${v}` : '';
        },
      },
    ],
    [],
  );

  const table = useReactTable({
    data: pagedData,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <>
      <TableContainer
        component={Paper}
        sx={{ mt: 4, mb: 1, pb: 1, boxShadow: 'none', borderRadius: 4, backgroundImage: 'none' }}
      >
        <Table size="small" sx={{ fontSize: '12px' }}>
          <TableHead>
            {table.getHeaderGroups().map((headerGroup) => (
              <TableRow key={headerGroup.id} sx={{ borderBottom: 1, borderColor: 'divider' }}>
                {headerGroup.headers.map((header) => (
                  <TableCell
                    key={header.id}
                    align="left"
                    sx={(theme) => ({
                      fontSize: '12px',
                      borderBottom: 1,
                      borderColor: 'divider',
                      fontWeight: 'bold',
                      color: theme.palette.primary.main,
                    })}
                  >
                    {flexRender(header.column.columnDef.header, header.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableHead>
          <TableBody>
            {table.getRowModel().rows.map((row) => (
              <TableRow key={row.id} sx={{ borderBottom: 'none' }}>
                {row.getVisibleCells().map((cell) => (
                  <TableCell
                    key={cell.id}
                    align="left"
                    sx={{ fontSize: '12px', borderBottom: 'none' }}
                  >
                    {flexRender(
                      cell.column.columnDef.cell || cell.column.columnDef.header,
                      cell.getContext(),
                    )}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
      {/* Pagination controls */}
      <Divider sx={{ mt: 2, mb: 2 }} />
      <Box display="flex" alignItems="center" justifyContent="flex-end" gap={2} mb={4}>
        <Typography
          variant="body2"
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
          })}
        >
          Rows per page:
        </Typography>
        <Select
          size="small"
          value={pageSize}
          onChange={(e) => {
            setPageSize(Number(e.target.value));
            setPage(0);
          }}
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
            '.MuiOutlinedInput-notchedOutline': {
              borderColor:
                theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.2)' : 'rgba(0,0,0,0.2)',
            },
          })}
        >
          {[5, 10, 25, 50, 100].map((size) => (
            <MenuItem key={size} value={size} sx={{ color: 'inherit' }}>
              {size}
            </MenuItem>
          ))}
        </Select>
        <Typography
          variant="body2"
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
          })}
        >
          {page * pageSize + 1}-{Math.min((page + 1) * pageSize, totalRows)} of {totalRows}
        </Typography>
        <Button
          size="small"
          onClick={() => setPage((p) => Math.max(0, p - 1))}
          disabled={page === 0}
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
          })}
        >
          Prev
        </Button>
        <Button
          size="small"
          onClick={() => setPage((p) => Math.min(pageCount - 1, p + 1))}
          disabled={page >= pageCount - 1}
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
          })}
        >
          Next
        </Button>
      </Box>
    </>
  );
}
