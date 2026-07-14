import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from 'recharts';

export type Point = {
  time: string;
  depth: number;
  p50: number;
  p95: number;
  p99: number;
  throughput: number;
};

export default function Chart({ data, lines }: { data: Point[]; lines: { key: keyof Point; color: string; name: string }[] }) {
  return (
    <div className="h-64">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={data}>
          <CartesianGrid stroke="rgba(255,255,255,0.06)" vertical={false} />
          <XAxis dataKey="time" tick={{ fill: '#71717a', fontSize: 11 }} interval="preserveStartEnd" minTickGap={36} />
          <YAxis tick={{ fill: '#71717a', fontSize: 11 }} width={42} />
          <Tooltip contentStyle={{ background: '#111315', border: '1px solid rgba(255,255,255,0.1)', borderRadius: 6, color: '#f4f4f5' }} labelStyle={{ color: '#a1a1aa' }} />
          {lines.map((line) => <Line key={line.key} type="monotone" dataKey={line.key} name={line.name} stroke={line.color} strokeWidth={2} dot={false} isAnimationActive={false} />)}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
