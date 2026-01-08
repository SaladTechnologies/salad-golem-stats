# Salad Stats Frontend

React dashboard built with Vite and Material-UI.

## Development

```bash
npm install
npm run dev
```

The dev server runs at http://localhost:5173 with hot reload.

## Production Build

```bash
npm run build
```

Output is written to `dist/`.

## Deployment

This is a static single-page app that can be deployed to any static hosting service.

### Netlify / Vercel / Cloudflare Pages

| Setting | Value |
|---------|-------|
| Build command | `npm run build` |
| Publish directory | `dist` |
| Node version | 18+ |

Set the `VITE_STATS_API_URL` environment variable in your hosting provider's dashboard to point to your backend API.

### Manual Deployment

Upload the contents of `dist/` to any static file host (S3, GitHub Pages, etc.).

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `VITE_STATS_API_URL` | `http://localhost:8000` | Backend API URL |

For local development, create a `.env` file:

```bash
cp .env.example .env
```

For production, set environment variables in your hosting provider's dashboard. Vite embeds these at build time.

## Other Commands

```bash
npm run lint          # Run ESLint
npm run format        # Format code with Prettier
npm run format:check  # Check formatting
npm run preview       # Preview production build locally
```
