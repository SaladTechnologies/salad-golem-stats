import { useState, useCallback, useEffect, useMemo } from 'react';
import { useTheme } from '@mui/material';
import ArrowDropUpIcon from '@mui/icons-material/ArrowDropUp';
import ArrowDropDownIcon from '@mui/icons-material/ArrowDropDown';
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  ColumnDef,
  CellContext,
} from '@tanstack/react-table';
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
  TablePagination,
} from '@mui/material';

export interface Transaction {
  tx_hash?: string;
  block_number?: number;
  block_timestamp?: string;
  from_address?: string;
  to_address?: string;
  value_glm?: number;
  tx_type?: string;
}

interface SortableHeaderProps {
  label: string;
  sortKey: string;
  sortBy: string;
  sortOrder: 'asc' | 'desc';
  setSortBy: (key: string) => void;
  setSortOrder: (order: 'asc' | 'desc') => void;
}

interface FetchOptions {
  page?: number;
  pageSize?: number;
  sortBy?: string;
  sortOrder?: 'asc' | 'desc';
}

interface TransactionsResponse {
  transactions?: Transaction[];
  total?: number;
}

export default function TransactionsTable() {
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(10);
  const [totalRows, setTotalRows] = useState(0);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(false);
  const [sortBy, setSortBy] = useState('time');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');

  const fetchTransactions = useCallback(
    (opts: FetchOptions = {}) => {
      setLoading(true);
      const params = new URLSearchParams();
      params.append('limit', String(opts.pageSize ?? pageSize));
      // Calculate offset for page-based pagination
      const offset = (opts.page ?? page) * (opts.pageSize ?? pageSize);
      params.append('offset', String(offset));
      params.append('sort_by', opts.sortBy ?? sortBy);
      params.append('sort_order', opts.sortOrder ?? sortOrder);
      fetch(`${import.meta.env.VITE_STATS_API_URL}/metrics/transactions?${params.toString()}`)
        .then((res) =>
          res.ok
            ? (res.json() as Promise<TransactionsResponse>)
            : Promise.reject('Failed to fetch transactions'),
        )
        .then((data) => {
          if (data.transactions) {
            setTransactions(data.transactions);
            setTotalRows(data.total ?? 0);
          }
        })
        .catch(() => {
          setTransactions([]);
          setTotalRows(0);
        })
        .finally(() => setLoading(false));
    },
    [page, pageSize, sortBy, sortOrder],
  );

  // Initial load and when page, pageSize, sortBy, or sortOrder changes
  useEffect(() => {
    fetchTransactions({ page, pageSize, sortBy, sortOrder });
  }, [page, pageSize, sortBy, sortOrder, fetchTransactions]);

  // SortableHeader component for column headers
  function SortableHeader({
    label,
    sortKey,
    sortBy,
    sortOrder,
    setSortBy,
    setSortOrder,
  }: SortableHeaderProps) {
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
    const [hover, setHover] = useState(false);
    const arrowColor = hover ? 'rgb(83,166,38)' : theme.palette.mode === 'dark' ? '#444' : '#ddd';
    return (
      <span
        className={`sortable-header${isActive ? ' active' : ''}`}
        onClick={handleClick}
        onMouseEnter={() => setHover(true)}
        onMouseLeave={() => setHover(false)}
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
                transition: 'color 0.2s',
              }}
            />
            <ArrowDropDownIcon
              style={{
                color: arrowColor,
                fontSize: '1.75em',
                marginBottom: '-00em',
                marginTop: '-.3em',
                transition: 'color 0.2s',
              }}
            />
          </span>
        )}
      </span>
    );
  }

  const columns: ColumnDef<Transaction>[] = useMemo(
    () => [
      {
        accessorKey: 'block_timestamp',
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
        cell: (info: CellContext<Transaction, unknown>) => {
          const value = info.getValue() as string | undefined;
          return value ? String(value).replace('T', ' ').replace('Z', '') : '';
        },
      },
      {
        accessorKey: 'from_address',
        header: 'From',
        cell: (info: CellContext<Transaction, unknown>) => {
          const v = info.getValue() as string | undefined;
          return v ? String(v).slice(0, 10) + '...' : '';
        },
      },
      {
        accessorKey: 'to_address',
        header: 'To',
        cell: (info: CellContext<Transaction, unknown>) => {
          const v = info.getValue() as string | undefined;
          return v ? String(v).slice(0, 10) + '...' : '';
        },
      },
      {
        accessorKey: 'tx_hash',
        header: 'Transaction Hash',
        cell: (info: CellContext<Transaction, unknown>) => {
          const v = info.getValue() as string | undefined;
          if (!v) return '';
          const short = String(v).slice(0, 10) + '...';
          return (
            <a
              href={`https://etherscan.io/tx/${v}`}
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
        accessorKey: 'value_glm',
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
        cell: (info: CellContext<Transaction, unknown>) => {
          const v = info.getValue() as number | undefined;
          return v !== undefined && v !== null ? v.toFixed(4) : '';
        },
      },
    ],
    [sortBy, sortOrder],
  );

  const table = useReactTable({
    data: transactions,
    columns,
    getCoreRowModel: getCoreRowModel(),
    manualPagination: true,
    rowCount: totalRows,
  });

  const handleChangePage = (
    _event: React.MouseEvent<HTMLButtonElement> | null,
    newPage: number,
  ) => {
    setPage(newPage);
  };

  const handleChangeRowsPerPage = (
    event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>,
  ) => {
    setPageSize(parseInt(event.target.value, 10));
    setPage(0);
  };

  return (
    <>
      <TablePagination
        component="div"
        count={totalRows}
        page={page}
        onPageChange={handleChangePage}
        rowsPerPage={pageSize}
        onRowsPerPageChange={handleChangeRowsPerPage}
        rowsPerPageOptions={[5, 10, 25, 50, 100]}
        showFirstButton
        showLastButton
      />
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
      <Typography
        variant="caption"
        sx={(theme) => ({
          display: 'block',
          textAlign: 'right',
          mt: 1,
          color: theme.palette.mode === 'dark' ? 'rgba(255,255,255,0.4)' : 'rgba(0,0,0,0.4)',
        })}
      >
        Powered by{' '}
        <a
          href="https://etherscan.io"
          target="_blank"
          rel="noopener noreferrer"
          style={{ color: 'inherit' }}
        >
          Etherscan.io
        </a>{' '}
        APIs
      </Typography>
    </>
  );
}
