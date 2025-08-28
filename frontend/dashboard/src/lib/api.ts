const base = process.env.FRONTEND_BASE_URL

export type GMVPoint = { day_utc: string; currency: string; tx_count: number; total_amount: string };
export type MerchantRow = { merchant_name_norm: string; tx_count: number; total_amount: string };
export type RiskFlag = {
  event_id: string; account_id: string;
  window_start: string; window_end: string;
  count_5m: number; sum_5m: string; reason: string; created_at: string;
};

export async function fetchGMV(currency = "VND", days = 30, options: RequestInit = {}) {
  const res = await fetch(`${base}/api/gmv?currency=${currency}&days=${days}`, {
    ...options,
    // Revalidate every 60s on the server
    next: { revalidate: 60 },
  });
  if (!res.ok) throw new Error("Failed to fetch GMV");
  return (await res.json()) as GMVPoint[];
}

export async function fetchTopMerchants(limit = 10, options: RequestInit = {}) {
  const res = await fetch(`${base}/api/merchants?limit=${limit}`, {
    ...options,
    next: { revalidate: 120 },
  });
  if (!res.ok) throw new Error("Failed to fetch top merchants");
  return (await res.json()) as MerchantRow[];
}

// Client-side polling for risk flags
export async function fetchRiskFlags(hours = 1) {
  const res = await fetch(`/api/flags?hours=${hours}`, { cache: "no-store" });
  if (!res.ok) throw new Error("Failed to fetch risk flags");
  return (await res.json()) as RiskFlag[];
}
