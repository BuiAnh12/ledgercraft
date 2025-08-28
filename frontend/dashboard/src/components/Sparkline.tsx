"use client";
import { GMVPoint } from "@/lib/api";
import { useMemo } from "react";

export default function Sparkline({ data, height = 80 }: { data: GMVPoint[]; height?: number }) {
  const { points, min, max } = useMemo(() => {
    if (!data.length) return { points: "", min: 0, max: 0 };
    const values = data.map(d => Number(d.total_amount));
    const min = Math.min(...values);
    const max = Math.max(...values);
    const pad = 8;
    const w = Math.max(200, data.length * 8);
    const h = height;
    const scaleX = (i: number) => (i / (data.length - 1)) * (w - pad * 2) + pad;
    const scaleY = (v: number) => {
      if (max === min) return h / 2;
      return h - pad - ((v - min) / (max - min)) * (h - pad * 2);
    };
    const pts = data.map((d, i) => `${scaleX(i)},${scaleY(Number(d.total_amount))}`).join(" ");
    return { points: pts, min, max };
  }, [data, height]);

  return (
    <div className="w-full overflow-x-auto">
      <svg width={Math.max(200, data.length * 8)} height={height}>
        <polyline points={points} fill="none" stroke="currentColor" strokeWidth="2" />
      </svg>
      <div className="text-xs text-gray-500 mt-1">
        min: {min.toLocaleString()} • max: {max.toLocaleString()}
      </div>
    </div>
  );
}
