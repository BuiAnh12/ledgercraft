import Card from "@/components/Card";
import Sparkline from "@/components/Sparkline";
import MerchantsTable from "@/components/MerchantsTable";
import RiskFlagsList from "@/components/RiskFlagsList";
import { fetchGMV, fetchTopMerchants } from "@/lib/api";

export default async function Page() {
  const [gmvVND, topMerchants] = await Promise.all([
    fetchGMV("VND", 30, { cache: "no-store" }),    // override if you want always fresh SSR
    fetchTopMerchants(10, { cache: "no-store" }),
  ]);

  return (
    <main className="min-h-screen bg-background text-foreground">
      <div className="max-w-6xl mx-auto p-6 space-y-6">
        <h1 className="text-2xl font-bold">LedgerCraft Dashboard</h1>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <Card title="Daily GMV (VND, last 30 days)">
            <Sparkline data={gmvVND} />
          </Card>

          <Card title="Top Merchants (7d)">
            <MerchantsTable rows={topMerchants} />
          </Card>

          <Card title="Recent Risk Flags (live)">
            <RiskFlagsList hours={1} refreshMs={5000} />
          </Card>
        </div>
      </div>
    </main>

  );
}
