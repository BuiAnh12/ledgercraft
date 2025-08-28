import { NextRequest, NextResponse } from "next/server";

const BASE = process.env.NEXT_PUBLIC_API_BASE_URL!;
const KEY  = process.env.READ_API_KEY!;

export async function GET(req: NextRequest) {
  const currency = req.nextUrl.searchParams.get("currency") ?? "VND";
  const days = req.nextUrl.searchParams.get("days") ?? "30";

  const res = await fetch(`${BASE}/gmv/daily?currency=${currency}&days=${days}`, {
    headers: { "X-API-Key": KEY },
    next: { revalidate: 60 }, // cache on server
  });
  if (!res.ok) return NextResponse.json({ error: "upstream failed" }, { status: 500 });
  const data = await res.json();
  return NextResponse.json(data);
}
