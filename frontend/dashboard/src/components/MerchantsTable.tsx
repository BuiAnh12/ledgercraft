import { MerchantRow } from "@/lib/api";

export default function MerchantsTable({ rows }: { rows: MerchantRow[] }) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead className="text-left text-gray-500">
          <tr>
            <th className="py-2 pr-4">Merchant</th>
            <th className="py-2 pr-4">Tx Count</th>
            <th className="py-2 pr-4">Total</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.merchant_name_norm} className="border-t">
              <td className="py-2 pr-4">{r.merchant_name_norm}</td>
              <td className="py-2 pr-4">{r.tx_count}</td>
              <td className="py-2 pr-4">{Number(r.total_amount).toLocaleString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {!rows.length && <div className="text-sm text-gray-400 mt-2">No data</div>}
    </div>
  );
}
