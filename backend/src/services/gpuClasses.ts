import { query } from '../db/connection.js';

interface GpuClass {
  gpu_class_id: string;
  gpu_class_name: string;
}

export const gpuClassNames: Map<string, string> = new Map();

export async function loadGpuClassNames(): Promise<void> {
  try {
    const rows = await query<GpuClass>(
      'SELECT gpu_class_id, gpu_class_name FROM gpu_classes'
    );
    for (const row of rows) {
      gpuClassNames.set(row.gpu_class_id, row.gpu_class_name);
    }
    console.log(`Loaded ${gpuClassNames.size} GPU class names`);
  } catch (err) {
    console.error('Warning: Could not load gpu_class_names at startup:', err);
  }
}
