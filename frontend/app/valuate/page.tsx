"use client";

import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import api from "@/lib/api";
import { Navbar } from "@/components/layout/navbar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { Sparkles, TrendingUp, TrendingDown, Minus, AlertCircle, Loader2 } from "lucide-react";

const fmt = (n: number) =>
  new Intl.NumberFormat("en-SG", { style: "currency", currency: "SGD", maximumFractionDigits: 0 }).format(n);

const fmtImpact = (n: number) => {
  const abs = Math.abs(n);
  const sign = n >= 0 ? "+" : "−";
  if (abs >= 1_000_000) return `${sign}S$${(abs / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000)     return `${sign}S$${Math.round(abs / 1_000)}K`;
  return `${sign}S$${abs.toLocaleString()}`;
};

interface ShapFactor {
  label:      string;
  impact_sgd: number;
  direction:  "positive" | "negative";
}
interface ValuationResult {
  estimate:    number;
  range_low:   number;
  range_high:  number;
  mape:        number;
  shap_factors: ShapFactor[];
  disclaimer:  string;
}

export default function ValuatePage() {
  const [form, setForm] = useState({
    property_type: "Condominium",
    buy_rent:      "property-for-sale",
    beds:          "3",
    sqft:          "1000",
    tenure:        "Freehold",
  });

  const mutation = useMutation<ValuationResult, Error, typeof form>({
    mutationFn: async (vals) => {
      const { data } = await api.post("/valuation/estimate", {
        property_type: vals.property_type,
        buy_rent:      vals.buy_rent,
        beds:          Number(vals.beds),
        sqft:          Number(vals.sqft),
        tenure:        vals.tenure || null,
      });
      return data;
    },
  });

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    mutation.mutate(form);
  };

  const data = mutation.data;

  return (
    <div className="min-h-screen bg-background">
      <Navbar />

      <div className="container mx-auto px-4 py-12 max-w-4xl">
        {/* Hero */}
        <div className="mb-10 text-center">
          <div className="inline-flex items-center gap-2 bg-violet-100 text-violet-700 px-4 py-1.5 rounded-full text-sm font-semibold mb-4">
            <Sparkles className="h-4 w-4" /> AI-Powered
          </div>
          <h1 className="text-4xl font-extrabold tracking-tight mb-3">
            Property Valuation
          </h1>
          <p className="text-muted-foreground text-lg max-w-xl mx-auto">
            Get an instant AI estimate for any Singapore property. Enter the key details below.
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-10 items-start">
          {/* Form */}
          <form onSubmit={handleSubmit} className="space-y-5 bg-white border rounded-2xl p-8 shadow-sm">
            <h2 className="font-semibold text-lg">Property Details</h2>

            <div className="grid gap-1.5">
              <Label>Transaction Type</Label>
              <Select value={form.buy_rent} onValueChange={(v) => setForm(f => ({ ...f, buy_rent: v }))}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="property-for-sale">For Sale</SelectItem>
                  <SelectItem value="property-for-rent">For Rent</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="grid gap-1.5">
              <Label>Property Type</Label>
              <Select value={form.property_type} onValueChange={(v) => setForm(f => ({ ...f, property_type: v }))}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="Condominium">Condominium</SelectItem>
                  <SelectItem value="HDB">HDB</SelectItem>
                  <SelectItem value="Landed">Landed</SelectItem>
                  <SelectItem value="Good Class Bungalow">Good Class Bungalow</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="grid gap-1.5">
              <Label>Tenure</Label>
              <Select value={form.tenure} onValueChange={(v) => setForm(f => ({ ...f, tenure: v }))}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="Freehold">Freehold</SelectItem>
                  <SelectItem value="Leasehold 99">Leasehold 99-year</SelectItem>
                  <SelectItem value="Leasehold 999">Leasehold 999-year</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="grid grid-cols-2 gap-4">
              <div className="grid gap-1.5">
                <Label htmlFor="beds">Bedrooms</Label>
                <Input
                  id="beds" type="number" min={0} max={20} step={1}
                  value={form.beds}
                  onChange={(e) => setForm(f => ({ ...f, beds: e.target.value }))}
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="sqft">Floor Area (sqft)</Label>
                <Input
                  id="sqft" type="number" min={50} max={50000} step={10}
                  value={form.sqft}
                  onChange={(e) => setForm(f => ({ ...f, sqft: e.target.value }))}
                />
              </div>
            </div>

            <Button
              type="submit"
              size="lg"
              className="w-full bg-violet-600 hover:bg-violet-700 text-white"
              disabled={mutation.isPending}
            >
              {mutation.isPending ? (
                <><Loader2 className="mr-2 h-4 w-4 animate-spin" /> Estimating...</>
              ) : (
                <><Sparkles className="mr-2 h-4 w-4" /> Get Estimate</>
              )}
            </Button>

            {mutation.isError && (
              <div className="flex items-center gap-2 text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg p-3">
                <AlertCircle className="h-4 w-4 flex-shrink-0" />
                {mutation.error?.message || "Valuation failed. Ensure the model is trained."}
              </div>
            )}
          </form>

          {/* Result */}
          <div>
            {!data && !mutation.isPending && (
              <div className="flex flex-col items-center justify-center h-full text-center text-muted-foreground py-20 border-2 border-dashed rounded-2xl">
                <Sparkles className="h-10 w-10 mb-3 text-violet-300" />
                <p className="font-medium">Your estimate will appear here</p>
                <p className="text-sm mt-1">Fill in the details and click Get Estimate</p>
              </div>
            )}

            {mutation.isPending && (
              <div className="flex flex-col items-center justify-center h-full py-20 text-violet-500">
                <Loader2 className="h-8 w-8 animate-spin mb-3" />
                <p className="font-medium">Analysing with AI model…</p>
              </div>
            )}

            {data && (
              <div className="bg-gradient-to-br from-violet-50 to-white border rounded-2xl p-8 shadow-sm space-y-6">
                {/* Estimate */}
                <div>
                  <div className="text-sm text-muted-foreground mb-1">Market Estimate</div>
                  <div className="text-4xl font-extrabold text-violet-800">{fmt(data.estimate)}</div>
                  <div className="text-sm text-muted-foreground mt-1">
                    Confidence range: {fmt(data.range_low)} – {fmt(data.range_high)}
                    <span className="ml-2 text-xs">(±{Math.round(data.mape * 100)}%)</span>
                  </div>
                </div>

                {/* Range bar */}
                <div className="relative">
                  <div className="h-3 rounded-full bg-violet-100 relative overflow-hidden">
                    <div className="absolute inset-0 bg-gradient-to-r from-violet-200 via-violet-400 to-violet-200 rounded-full opacity-60" />
                    {/* Estimate marker */}
                    <div className="absolute top-0 bottom-0 w-1 bg-violet-700 rounded-full" style={{ left: "50%" }} />
                  </div>
                  <div className="flex justify-between text-xs text-muted-foreground mt-1">
                    <span>{fmt(data.range_low)}</span>
                    <span className="text-violet-700 font-semibold">{fmt(data.estimate)}</span>
                    <span>{fmt(data.range_high)}</span>
                  </div>
                </div>

                {/* SHAP Factors */}
                {data.shap_factors && data.shap_factors.length > 0 && (
                  <div className="space-y-3">
                    <h3 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
                      Key Value Drivers
                    </h3>
                    <div className="space-y-2">
                      {data.shap_factors.slice(0, 5).map((f, i) => (
                        f.label && f.impact_sgd != null ? (
                          <div key={i} className="flex items-center gap-3">
                            <div className={`flex-shrink-0 w-2 h-2 rounded-full ${f.direction === "positive" ? "bg-green-500" : "bg-red-400"}`} />
                            <div className="flex-1 flex items-center justify-between">
                              <span className="text-sm text-gray-700">{f.label}</span>
                              <span className={`text-sm font-mono font-semibold ${f.direction === "positive" ? "text-green-700" : "text-red-600"}`}>
                                {fmtImpact(f.impact_sgd)}
                              </span>
                            </div>
                          </div>
                        ) : null
                      ))}
                    </div>
                  </div>
                )}

                {/* Disclaimer */}
                <p className="text-xs text-muted-foreground border-t pt-4 leading-relaxed">
                  ⚠ {data.disclaimer}
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
