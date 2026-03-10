"use client";

import { useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import api from "@/lib/api";
import { Sparkles, TrendingUp, TrendingDown, Minus, AlertCircle, AlertTriangle } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

interface ShapFactor {
  label:      string;
  impact_sgd: number;
  direction:  "positive" | "negative";
  shap_value: number;
}

interface ValuationResult {
  estimate:     number;
  range_low:    number;
  range_high:   number;
  mode:         string;
  mape:         number;
  r2:           number | null;
  premium_pct:  number | null;
  verdict:      "overpriced" | "fair_value" | "below_estimate" | null;
  shap_factors: ShapFactor[];
  disclaimer:   string;
}

interface ValuationPanelProps {
  propertyType: string;
  buyRent:      string;
  beds:         number;
  sqft:         number;
  tenure?:      string;
  actualPrice?: number;
  builtYear?:   number;
  onResult?:    (result: ValuationResult) => void;
}

const fmt = (n: number) =>
  new Intl.NumberFormat("en-SG", { style: "currency", currency: "SGD", maximumFractionDigits: 0 }).format(n);

const fmtImpact = (n: number) => {
  const abs = Math.abs(n);
  const sign = n >= 0 ? "+" : "−";
  if (abs >= 1_000_000) return `${sign}S$${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000)     return `${sign}S$${Math.round(abs / 1_000)}K`;
  return `${sign}S$${abs.toLocaleString()}`;
};

const VerdictBadge = ({ verdict, premium }: { verdict: string | null; premium: number | null }) => {
  if (!verdict || premium == null) return null;
  const map = {
    overpriced:     { label: "Overpriced",      icon: TrendingUp,   cls: "bg-red-100 text-red-700 border-red-200" },
    fair_value:     { label: "Fair Value",       icon: Minus,        cls: "bg-yellow-100 text-yellow-700 border-yellow-200" },
    below_estimate: { label: "Below Estimate",   icon: TrendingDown, cls: "bg-green-100 text-green-700 border-green-200" },
  } as const;
  const v = map[verdict as keyof typeof map];
  if (!v) return null;
  const Icon = v.icon;
  return (
    <span className={`inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-sm font-semibold border ${v.cls}`}>
      <Icon className="h-3.5 w-3.5" />
      {v.label}
      <span className="font-normal opacity-75 text-xs">
        ({premium >= 0 ? "+" : ""}{premium}%)
      </span>
    </span>
  );
};

export function ValuationPanel({
  propertyType, buyRent, beds, sqft, tenure, actualPrice, builtYear, onResult,
}: ValuationPanelProps) {

  const enabled = !!(propertyType && buyRent && beds >= 1 && sqft > 50);

  const { data, isLoading, isError } = useQuery<ValuationResult>({
    queryKey: ["valuation", propertyType, buyRent, beds, sqft, tenure, actualPrice, builtYear],
    queryFn: async () => {
      const { data } = await api.post("/valuation/estimate", {
        property_type: propertyType,
        buy_rent:      buyRent,
        beds,
        sqft,
        tenure:       tenure ?? null,
        actual_price: actualPrice ?? null,
        built_year:   builtYear ?? null,
      });
      return data;
    },
    enabled,
    staleTime: 1000 * 60 * 10,
    retry: false,
  });

  // Lift valuation result up to parent (for chat panel context)
  useEffect(() => {
    if (data && onResult) onResult(data);
  }, [data]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!enabled) return null;

  return (
    <div className="rounded-xl border bg-gradient-to-br from-violet-50 to-white p-5 space-y-4">
      {/* Header */}
      <div className="flex items-center gap-2">
        <span className="flex items-center justify-center rounded-lg bg-violet-600 p-1.5">
          <Sparkles className="h-4 w-4 text-white" />
        </span>
        <h3 className="font-semibold text-base text-violet-900">AI Valuation</h3>
      </div>

      {isLoading && (
        <div className="space-y-3">
          <Skeleton className="h-8 w-40 bg-violet-100" />
          <Skeleton className="h-4 w-full bg-violet-100" />
          <Skeleton className="h-4 w-3/4 bg-violet-100" />
        </div>
      )}

      {isError && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <AlertCircle className="h-4 w-4 text-yellow-500" />
          Valuation model unavailable. Ensure the model is trained.
        </div>
      )}

      {data && (
        <div className="space-y-4">
          {/* Low-reliability warning for segments with R² < 0.3 (e.g. GCB Sale) */}
          {data.r2 !== null && data.r2 !== undefined && data.r2 < 0.3 && (
            <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2.5 text-xs text-amber-800">
              <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-500" />
              <span>
                <span className="font-semibold">Low model confidence</span> — this segment has
                limited training data (R²&nbsp;=&nbsp;{data.r2.toFixed(2)}).
                The estimate below is indicative only and carries high uncertainty.
              </span>
            </div>
          )}

          {/* Estimate */}
          <div>
            <div className="text-xs text-muted-foreground mb-0.5">Market Estimate</div>
            <div className="text-2xl font-bold text-violet-800">{fmt(data.estimate)}</div>
            <div className="text-xs text-muted-foreground mt-0.5">
              Range: {fmt(data.range_low)} – {fmt(data.range_high)}
            </div>
          </div>

          {/* Price range bar */}
          {data.estimate && (
            <div className="relative h-6 w-full">
              <div className="absolute inset-0 rounded-full bg-violet-100" />
              {/* Confidence range fill */}
              {(() => {
                const lo = data.range_low, hi = data.range_high, est = data.estimate;
                const actual = actualPrice;
                const span = hi - lo;
                const estPct = ((est - lo) / span) * 100;
                const actPct = actual ? ((actual - lo) / span) * 100 : null;
                return (
                  <>
                    <div className="absolute inset-y-0 left-0 right-0 rounded-full overflow-hidden">
                      <div className="h-full bg-violet-200 rounded-full" style={{ width: "100%" }} />
                    </div>
                    {/* Estimate marker */}
                    <div
                      className="absolute top-0 bottom-0 w-1 bg-violet-600 rounded-full"
                      style={{ left: `${Math.min(95, Math.max(5, estPct))}%` }}
                    />
                    {/* Actual price marker */}
                    {actPct != null && (
                      <div
                        className="absolute -top-0.5 -bottom-0.5 w-1.5 rounded-full bg-gray-800"
                        style={{ left: `${Math.min(95, Math.max(5, actPct))}%` }}
                        title={`Listed: ${fmt(actual!)}`}
                      />
                    )}
                  </>
                );
              })()}
              {/* Labels */}
              <div className="absolute -bottom-5 left-0 text-[10px] text-muted-foreground">{fmt(data.range_low)}</div>
              <div className="absolute -bottom-5 right-0 text-[10px] text-muted-foreground">{fmt(data.range_high)}</div>
            </div>
          )}

          {/* Verdict */}
          {data.verdict && (
            <div className="pt-4 space-y-1">
              <VerdictBadge verdict={data.verdict} premium={data.premium_pct} />
              {actualPrice && (
                <div className="text-xs text-muted-foreground">
                  Listed at {fmt(actualPrice)} vs estimate {fmt(data.estimate)}
                </div>
              )}
            </div>
          )}

          {/* SHAP Factors */}
          {data.shap_factors && data.shap_factors.length > 0 && (
            <div className="space-y-2 pt-1">
              <div className="text-xs font-semibold text-muted-foreground uppercase tracking-wide">
                Why this estimate?
              </div>
              <div className="space-y-1.5">
                {data.shap_factors.slice(0, 5).map((f, i) => (
                  f.label && f.impact_sgd != null ? (
                    <div key={i} className="flex items-center justify-between text-sm">
                      <span className="text-gray-700 truncate mr-2">{f.label}</span>
                      <span className={`font-mono font-medium text-xs whitespace-nowrap ${
                        f.direction === "positive" ? "text-green-700" : "text-red-600"
                      }`}>
                        {fmtImpact(f.impact_sgd)}
                      </span>
                    </div>
                  ) : null
                ))}
              </div>
            </div>
          )}

          {/* Disclaimer */}
          <p className="text-[11px] text-muted-foreground border-t pt-3 leading-relaxed">
            ⚠ {data.disclaimer}
          </p>
        </div>
      )}
    </div>
  );
}
