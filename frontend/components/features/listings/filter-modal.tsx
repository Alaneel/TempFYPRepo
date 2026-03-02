"use client";

import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { X } from "lucide-react";

export interface FilterValues {
  propertyType: string;
  buyRent: string;
  minPrice: string;
  maxPrice: string;
  beds: string;
}

interface FilterModalProps {
  open: boolean;
  onClose: () => void;
  values: FilterValues;
  onApply: (values: FilterValues) => void;
}

type TabId = "type" | "buyrent" | "price" | "beds";

const TABS: { id: TabId; label: string }[] = [
  { id: "type", label: "Property Type" },
  { id: "buyrent", label: "Buy / Rent" },
  { id: "price", label: "Price" },
  { id: "beds", label: "Bedrooms" },
];

const PROPERTY_TYPES = ["All", "Condo", "Landed", "HDB"];
const BUY_RENT_OPTIONS: { label: string; value: string }[] = [
  { label: "All", value: "all" },
  { label: "Buy", value: "sale" },
  { label: "Rent", value: "rent" },
];
const BED_OPTIONS = ["Any", "Studio", "1", "2", "3", "4", "5+"];

const PRICE_PRESETS = [
  { label: "< $500k", min: "", max: "500000" },
  { label: "$500k – $1m", min: "500000", max: "1000000" },
  { label: "$1m – $2m", min: "1000000", max: "2000000" },
  { label: "$2m – $5m", min: "2000000", max: "5000000" },
  { label: "> $5m", min: "5000000", max: "" },
];

function PillButton({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2 rounded-full border text-sm font-medium transition-colors ${
        active
          ? "bg-primary text-primary-foreground border-primary"
          : "bg-background text-foreground border-border hover:border-primary/60"
      }`}
    >
      {label}
    </button>
  );
}

export function FilterModal({ open, onClose, values, onApply }: FilterModalProps) {
  const [local, setLocal] = useState<FilterValues>(values);
  const [activeTab, setActiveTab] = useState<TabId>("type");

  // Sync local state whenever modal opens so it reflects latest applied filters
  useEffect(() => {
    if (open) setLocal(values);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!open) return null;

  const handleApply = () => {
    onApply(local);
    onClose();
  };

  const handleClear = () => {
    const cleared: FilterValues = {
      propertyType: "all",
      buyRent: "all",
      minPrice: "",
      maxPrice: "",
      beds: "any",
    };
    setLocal(cleared);
    onApply(cleared);
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/50 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative z-10 bg-background rounded-2xl shadow-2xl w-full max-w-lg overflow-hidden flex flex-col max-h-[90vh]">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b bg-foreground text-background rounded-t-2xl">
          <h2 className="text-lg font-bold">Filters</h2>
          <button onClick={onClose} className="hover:opacity-70 transition-opacity">
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b overflow-x-auto">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-5 py-3 text-sm font-medium whitespace-nowrap border-b-2 transition-colors ${
                activeTab === tab.id
                  ? "border-primary text-primary"
                  : "border-transparent text-muted-foreground hover:text-foreground"
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {activeTab === "type" && (
            <div>
              <p className="text-sm text-muted-foreground mb-4">Select property type</p>
              <div className="flex flex-wrap gap-2">
                {PROPERTY_TYPES.map((t) => (
                  <PillButton
                    key={t}
                    label={t}
                    active={local.propertyType === t || (t === "All" && local.propertyType === "all")}
                    onClick={() => setLocal({ ...local, propertyType: t === "All" ? "all" : t })}
                  />
                ))}
              </div>
            </div>
          )}

          {activeTab === "buyrent" && (
            <div>
              <p className="text-sm text-muted-foreground mb-4">Are you looking to buy or rent?</p>
              <div className="flex flex-wrap gap-2">
                {BUY_RENT_OPTIONS.map(({ label, value }) => (
                  <PillButton
                    key={value}
                    label={label}
                    active={local.buyRent === value}
                    onClick={() => setLocal({ ...local, buyRent: value })}
                  />
                ))}
              </div>
            </div>
          )}

          {activeTab === "price" && (
            <div className="space-y-6">
              <div>
                <p className="text-sm text-muted-foreground mb-4">Quick select</p>
                <div className="flex flex-wrap gap-2">
                  {PRICE_PRESETS.map((preset) => (
                    <PillButton
                      key={preset.label}
                      label={preset.label}
                      active={local.minPrice === preset.min && local.maxPrice === preset.max}
                      onClick={() =>
                        setLocal({ ...local, minPrice: preset.min, maxPrice: preset.max })
                      }
                    />
                  ))}
                </div>
              </div>
              <div>
                <p className="text-sm text-muted-foreground mb-3">Or enter a custom range</p>
                <div className="flex items-center gap-3">
                  <div className="flex-1">
                    <label className="text-xs text-muted-foreground mb-1 block">Min (SGD)</label>
                    <Input
                      type="number"
                      placeholder="e.g. 500000"
                      value={local.minPrice}
                      onChange={(e) => setLocal({ ...local, minPrice: e.target.value })}
                      className="h-10"
                    />
                  </div>
                  <span className="text-muted-foreground mt-5">–</span>
                  <div className="flex-1">
                    <label className="text-xs text-muted-foreground mb-1 block">Max (SGD)</label>
                    <Input
                      type="number"
                      placeholder="e.g. 2000000"
                      value={local.maxPrice}
                      onChange={(e) => setLocal({ ...local, maxPrice: e.target.value })}
                      className="h-10"
                    />
                  </div>
                </div>
              </div>
            </div>
          )}

          {activeTab === "beds" && (
            <div>
              <p className="text-sm text-muted-foreground mb-4">Minimum number of bedrooms</p>
              <div className="flex flex-wrap gap-2">
                {BED_OPTIONS.map((b) => (
                  <PillButton
                    key={b}
                    label={b}
                    active={
                      local.beds === b ||
                      (b === "Any" && (local.beds === "any" || local.beds === ""))
                    }
                    onClick={() => setLocal({ ...local, beds: b === "Any" ? "any" : b })}
                  />
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t flex gap-3">
          <Button variant="outline" className="flex-1 rounded-full" onClick={handleClear}>
            Clear All
          </Button>
          <Button className="flex-1 rounded-full" onClick={handleApply}>
            Apply
          </Button>
        </div>
      </div>
    </div>
  );
}
