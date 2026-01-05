import Fastify from 'fastify';
import cors from '@fastify/cors';
import { config } from './config.js';
import { initRedis } from './cache/redis.js';
import { loadGpuClassNames } from './services/gpuClasses.js';
import { registerRoutes } from './routes/index.js';

async function main() {
  const fastify = Fastify({
    logger: true,
  });

  // Register CORS
  await fastify.register(cors, {
    origin: config.frontendOrigins,
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  });

  // Initialize Redis
  await initRedis();

  // Load GPU class names
  await loadGpuClassNames();

  // Register routes
  await registerRoutes(fastify);

  // Start server
  try {
    await fastify.listen({ port: config.port, host: '0.0.0.0' });
    console.log(`Server running on http://0.0.0.0:${config.port}`);
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

main();
