"use client";

import { useEffect, useState } from "react";
import { RiskFlag, fetchRiskFlags } from "@/lib/api";

export default function RiskFlagsList({
  hours = 1,
  refreshMs = 5000,
}: { hours?: number; refreshMs?: number }) {
  const [rows, setRows] = useState<RiskFlag[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  async function load() {
    try {
      const data = await fetchRiskFlags(hours);
      setRows(data);
      setError(null);
    } catch (e: any) {
      setError(e.message || "error");
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => {
    load();
    const t = setInterval(load, refreshMs);
    return () => clearInterval(t);
  }, [hours, refreshMs]);

  if (loading) return <div className="text-sm text-foreground/60">Loading…</div>;
  if (error) return <div className="text-sm text-danger">Error: {error}</div>;

  return (
    <div className="space-y-2">
      {rows.map((r) => (
        <div key={r.event_id} className="rounded-xl border border-card-border bg-card p-3">
          <div className="flex items-center justify-between">
            <div className="font-medium">{r.reason.replace("_", " ")}</div>
            <div className="text-xs text-foreground/60">
              {new Date(r.created_at).toLocaleString()}
            </div>
          </div>

          <div className="text-sm text-foreground/80 mt-1">
            acct <span className="font-mono">{r.account_id.slice(0, 8)}…</span> •
            {" "}count {r.count_5m} • sum {Number(r.sum_5m).toLocaleString()}
          </div>

          <div className="text-xs text-foreground/60">
            window {new Date(r.window_start).toLocaleTimeString()}–{new Date(r.window_end).toLocaleTimeString()}
          </div>
        </div>
      ))}

      {!rows.length && (
        <div className="text-sm text-foreground/40">No recent flags</div>
      )}
    </div>
  );
}
