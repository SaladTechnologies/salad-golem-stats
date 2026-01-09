// Data generation functions

export interface DataPoint {
  x: number;
  y: number;
}

export interface StackedDataset {
  label: string;
  data: number[];
  backgroundColor: string;
  borderColor: string;
  borderWidth: number;
  fill: boolean;
}

export interface StackedData {
  labels: number[];
  datasets: StackedDataset[];
}

export function generateRandomData(numPoints = 100): DataPoint[] {
  const data: DataPoint[] = [];
  for (let i = 0; i < numPoints; i++) {
    data.push({ x: i, y: Math.floor(Math.random() * 100) });
  }
  return data;
}

export function generateStackedData(numPoints = 100, numSeries = 5): StackedData {
  const colors = [
    'rgba(255, 99, 132, 0.6)',
    'rgba(54, 162, 235, 0.6)',
    'rgba(255, 206, 86, 0.6)',
    'rgba(75, 192, 192, 0.6)',
    'rgba(153, 102, 255, 0.6)',
  ];
  const borderColors = [
    'rgba(255, 99, 132, 1)',
    'rgba(54, 162, 235, 1)',
    'rgba(255, 206, 86, 1)',
    'rgba(75, 192, 192, 1)',
    'rgba(153, 102, 255, 1)',
  ];
  const labels = Array.from({ length: numPoints }, (_, i) => i);
  const datasets: StackedDataset[] = [];
  for (let s = 0; s < numSeries; s++) {
    const data = Array.from({ length: numPoints }, () => Math.floor(Math.random() * 50 + s * 10));
    datasets.push({
      label: `Series ${s + 1}`,
      data,
      backgroundColor: colors[s],
      borderColor: borderColors[s],
      borderWidth: 1,
      fill: true,
    });
  }
  return { labels, datasets };
}
