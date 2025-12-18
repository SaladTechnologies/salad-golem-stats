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
} from '@mui/material';

export default function TransactionsTable({ data }) {
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
      { accessorKey: 'gpu', header: 'GPU' },
      {
        accessorKey: 'ram',
        header: 'RAM (GB)',
        cell: (info) => {
          const v = info.getValue();
          return v ? Math.round(Number(v) / 1024) : '';
        },
      },
      { accessorKey: 'vcpus', header: 'vCPUs' },
      { accessorKey: 'duration', header: 'Duration' },
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
    data: data || [],
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  return (
    <TableContainer
      component={Paper}
      sx={{ mt: 4, mb: 4, boxShadow: 'none', borderRadius: 4, backgroundImage: 'none' }}
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
  );
}
