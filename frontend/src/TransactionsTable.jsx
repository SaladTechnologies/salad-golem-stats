import React from 'react';
import { useTheme } from '@mui/material';
import ArrowDropUpIcon from '@mui/icons-material/ArrowDropUp';
import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
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

export default function TransactionsTable() {
  // Pagination and sorting state for cursor-based API
  const [pageSize, setPageSize] = React.useState(10);
  const [transactions, setTransactions] = React.useState([]);
  const [nextCursor, setNextCursor] = React.useState(null);
  const [prevCursor, setPrevCursor] = React.useState(null);
  const [currentCursor, setCurrentCursor] = React.useState(null);
  const [direction, setDirection] = React.useState('next');
  const [totalRows, setTotalRows] = React.useState(0);
  const [loading, setLoading] = React.useState(false);
  const [sortBy, setSortBy] = React.useState('time');
  const [sortOrder, setSortOrder] = React.useState('desc');

  // Fetch transactions from backend
  const fetchTransactions = React.useCallback(
    (opts = {}) => {
      setLoading(true);
      const params = new URLSearchParams();
      params.append('limit', opts.pageSize ?? pageSize);
      if (opts.cursor) params.append('cursor', opts.cursor);
      if (opts.direction) params.append('direction', opts.direction);
      params.append('sort_by', opts.sortBy ?? sortBy);
      params.append('sort_order', opts.sortOrder ?? sortOrder);
      fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/transactions?${params.toString()}`)
        .then((res) => (res.ok ? res.json() : Promise.reject('Failed to fetch transactions')))
        .then((data) => {
          if (data.transactions && data.transactions.length > 0) {
            setTransactions(data.transactions);
            setNextCursor(data.next_cursor || null);
            setPrevCursor(data.prev_cursor || null);
            setTotalRows(data.total || 0);
            setCurrentCursor(opts.cursor || null);
            setDirection(opts.direction || 'next');
          } else {
            // Optionally, show a message or flash a warning here
            // Do not update state, so user stays on last valid page
          }
        })
        .catch((err) => {
          setTransactions([]);
          setNextCursor(null);
          setPrevCursor(null);
          setTotalRows(0);
        })
        .finally(() => setLoading(false));
    },
    [pageSize, sortBy, sortOrder],
  );

  // Initial load and when pageSize, sortBy, or sortOrder changes
  React.useEffect(() => {
    fetchTransactions({ pageSize, cursor: null, direction: 'next', sortBy, sortOrder });
  }, [pageSize, sortBy, sortOrder, fetchTransactions]);

  const columns = React.useMemo(
    () => [
      {
        accessorKey: 'ts',
        header: () => (
          <SortableHeader
            label="Timestamp (UTC)"
            sortKey="time"
            sortBy={sortBy}
            sortOrder={sortOrder}
            setSortBy={setSortBy}
            setSortOrder={setSortOrder}
          />
        ),
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
      {
        accessorKey: 'invoiced_glm',
        header: () => (
          <SortableHeader
            label="GLM"
            sortKey="glm"
            sortBy={sortBy}
            sortOrder={sortOrder}
            setSortBy={setSortBy}
            setSortOrder={setSortOrder}
          />
        ),
      },
      {
        accessorKey: 'invoiced_dollar',
        header: () => (
          <SortableHeader
            label="USD"
            sortKey="usd"
            sortBy={sortBy}
            sortOrder={sortOrder}
            setSortBy={setSortBy}
            setSortOrder={setSortOrder}
          />
        ),
        cell: (info) => {
          const v = info.getValue();
          return v !== undefined && v !== null ? `$${v}` : '';
        },
      },
    ],
    [sortBy, sortOrder],
  );

  const table = useReactTable({
    data: transactions,
    columns,
    getCoreRowModel: getCoreRowModel(),
  });

  // SortableHeader component for column headers
  function SortableHeader({ label, sortKey, sortBy, sortOrder, setSortBy, setSortOrder }) {
    const isActive = sortBy === sortKey;
    const handleClick = () => {
      if (isActive) {
        setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
      } else {
        setSortBy(sortKey);
        setSortOrder('desc');
      }
    };
    // Use MUI theme to reactively update arrow color
    const theme = useTheme();
    const arrowColor = theme.palette.mode === 'dark' ? '#444' : '#ddd';
    return (
      <span
        className={`sortable-header${isActive ? ' active' : ''}`}
        onClick={handleClick}
        style={{
          cursor: 'pointer',
          display: 'inline-flex',
          alignItems: 'center',
          userSelect: 'none',
        }}
      >
        {label}
        {isActive ? (
          sortOrder === 'asc' ? (
            <ArrowDropUpIcon className="sort-arrow active" style={{ color: '#00c853' }} />
          ) : (
            <ArrowDropDownIcon className="sort-arrow active" style={{ color: '#00c853' }} />
          )
        ) : (
          <span
            className="sort-arrow gray"
            style={{
              display: 'flex',
              flexDirection: 'column',
              marginLeft: 2,
              alignItems: 'center',
              height: 18,
              justifyContent: 'center',
            }}
          >
            <ArrowDropUpIcon
              style={{
                color: arrowColor,
                fontSize: '1.75em',
                marginTop: '-0em',
                marginBottom: '-.3em',
              }}
            />
            <ArrowDropDownIcon
              style={{
                color: arrowColor,
                fontSize: '1.75em',
                marginBottom: '-00em',
                marginTop: '-.3em',
              }}
            />
          </span>
        )}
      </span>
    );
  }

  return (
    <>
      {/* Pagination controls at the top */}
      <Box display="flex" alignItems="center" justifyContent="flex-end" gap={2} mb={2} mt={2}>
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
        <Button
          size="small"
          variant="outlined"
          onClick={() => {
            fetchTransactions({
              pageSize,
              cursor: null,
              direction: 'next',
            });
          }}
          disabled={!prevCursor}
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
            borderColor:
              theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.12)',
            background: 'none',
            boxShadow: 'none',
            textTransform: 'none',
          })}
        >
          First
        </Button>
        <Button
          size="small"
          variant="outlined"
          onClick={() => {
            fetchTransactions({
              pageSize,
              cursor: prevCursor,
              direction: 'prev',
            });
          }}
          disabled={!prevCursor}
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
            borderColor:
              theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.12)',
            background: 'none',
            boxShadow: 'none',
            textTransform: 'none',
          })}
        >
          Previous
        </Button>
        <Button
          size="small"
          variant="outlined"
          onClick={() => {
            fetchTransactions({
              pageSize,
              cursor: nextCursor,
              direction: 'next',
            });
          }}
          disabled={!nextCursor}
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
            borderColor:
              theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.12)',
            background: 'none',
            boxShadow: 'none',
            textTransform: 'none',
          })}
        >
          Next
        </Button>
        <Button
          size="small"
          variant="outlined"
          onClick={() => {
            fetchTransactions({
              pageSize,
              cursor: null,
              direction: 'prev',
            });
          }}
          disabled={!nextCursor}
          sx={(theme) => ({
            color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.6)' : 'rgba(0,0,0,0.6)',
            borderColor:
              theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.15)' : 'rgba(0,0,0,0.12)',
            background: 'none',
            boxShadow: 'none',
            textTransform: 'none',
          })}
        >
          Last
        </Button>
      </Box>
      <Divider sx={{ mt: 2, mb: 2 }} />
      <TableContainer
        component={Paper}
        sx={{ mt: 0, mb: 1, pb: 1, boxShadow: 'none', borderRadius: 4, backgroundImage: 'none' }}
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
            {loading ? (
              <TableRow>
                <TableCell colSpan={columns.length} align="center">
                  Loading...
                </TableCell>
              </TableRow>
            ) : (
              table.getRowModel().rows.map((row) => (
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
              ))
            )}
          </TableBody>
        </Table>
      </TableContainer>
    </>
  );
}
