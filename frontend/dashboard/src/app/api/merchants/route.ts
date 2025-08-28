import { NextRequest, NextResponse } from "next/server";

// Force Node.js runtime so local/TCPSockets + process.env are reliable
export const runtime = "nodejs";

// OPTIONAL: cache the whole route for 120s (don’t also set next:{revalidate} on fetch)
export const revalidate = 120;

export async function GET(req: NextRequest) {
    const base = process.env.READ_API_BASE_URL //?? process.env.NEXT_PUBLIC_API_BASE_URL; // fallback if you must
    const key = process.env.READ_API_KEY;
    
    if (!base) {
        return NextResponse.json({ error: "Missing READ_API_BASE_URL" }, { status: 500 });
    }
    if (!key) {
        return NextResponse.json({ error: "Missing READ_API_KEY" }, { status: 500 });
    }

    const limit = req.nextUrl.searchParams.get("limit") ?? "10";
    const url = `${base}/merchants/top?limit=${encodeURIComponent(limit)}`;

    const upstream = await fetch(url, {
        headers: { "X-API-Key": key },
        // no need for next:{ revalidate } if you export revalidate above
    });

    if (!upstream.ok) {
        const body = await upstream.text().catch(() => "");
        return NextResponse.json(
            { error: "upstream failed", status: upstream.status, body },
            { status: 500 }
        );
    }

    const data = await upstream.json();
    return NextResponse.json(data, { status: 200 });
}
