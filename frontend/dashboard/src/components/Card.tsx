import { ReactNode } from "react";

export default function Card({ title, children }: { title: string; children: ReactNode }) {
    return (
        <div className="rounded-2xl border border-card-border bg-card shadow-sm p-4">
            <h2 className="text-lg font-semibold mb-3">{title}</h2>
            {children}
        </div>
    );
}