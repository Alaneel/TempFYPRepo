"use client";

import { useQuery } from "@tanstack/react-query";
import { useSearchParams, useRouter } from "next/navigation";
import { useState, useEffect, Suspense } from "react";
import api from "@/lib/api";
import { Listing, PaginatedResponse } from "@/types";
import { ListingCard } from "@/components/features/listings/listing-card";
import { Navbar } from "@/components/layout/navbar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Label } from "@/components/ui/label";
import MapView from "@/components/features/map/map-view";
import { Sparkles, Search, X } from "lucide-react";

const DEFAULT_CENTER: [number, number] = [1.3521, 103.8198];

// Parsed filter tags from semantic search
type ParsedFilters = Record<string, string | number | null>;

function FilterTag({
  label,
  value,
  onRemove,
}: {
  label: string;
  value: string | number;
  onRemove?: () => void;
}) {
  return (
    <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full bg-violet-100 text-violet-800 text-xs font-medium border border-violet-200">
      <span className="text-violet-500">{label}:</span> {String(value)}
      {onRemove && (
        <button onClick={onRemove} className="ml-0.5 hover:text-violet-900">
          <X className="h-3 w-3" />
        </button>
      )}
    </span>
  );
}

function ListingsContent() {
  const searchParams = useSearchParams();
  const router = useRouter();

  // ── Search mode ───────────────────────────────────────────────
  const [aiMode, setAiMode] = useState(searchParams.get("mode") === "ai");
  const [q, setQ] = useState(searchParams.get("q") || "");
  const [propertyType, setPropertyType] = useState(
    searchParams.get("property_type") || "all",
  );
  const [buyRent, setBuyRent] = useState(searchParams.get("buy_rent") || "all");
  const [page, setPage] = useState(Number(searchParams.get("page")) || 1);

  // Parsed filters returned by semantic search (for display)
  const [parsedFilters, setParsedFilters] = useState<ParsedFilters | null>(
    null,
  );

  // Sync state with URL params
  useEffect(() => {
    setQ(searchParams.get("q") || "");
    setPropertyType(searchParams.get("property_type") || "all");
    setBuyRent(searchParams.get("buy_rent") || "all");
    setPage(Number(searchParams.get("page")) || 1);
    setAiMode(searchParams.get("mode") === "ai");
  }, [searchParams]);

  const updateFilters = (overrides?: {
    q?: string;
    propertyType?: string;
    buyRent?: string;
    page?: number;
    mode?: string;
  }) => {
    const nextQ = overrides?.q ?? q;
    const nextPropertyType = overrides?.propertyType ?? propertyType;
    const nextBuyRent = overrides?.buyRent ?? buyRent;
    const nextPage = overrides?.page ?? page;
    const nextMode = overrides?.mode ?? (aiMode ? "ai" : "");

    const params = new URLSearchParams(searchParams.toString());

    if (nextQ) params.set("q", nextQ);
    else params.delete("q");
    if (nextPropertyType && nextPropertyType !== "all")
      params.set("property_type", nextPropertyType);
    else params.delete("property_type");
    if (nextBuyRent && nextBuyRent !== "all")
      params.set("buy_rent", nextBuyRent);
    else params.delete("buy_rent");
    if (nextMode) params.set("mode", nextMode);
    else params.delete("mode");

    if (overrides?.page) {
      params.set("page", nextPage.toString());
    } else if (overrides) {
      params.set("page", "1");
      setPage(1);
    }

    router.push(`/listings?${params.toString()}`);
  };

  // ── API call ──────────────────────────────────────────────────
  const isSemanticMode = searchParams.get("mode") === "ai";
  const currentQ = searchParams.get("q") || "";

  const queryFn = async () => {
    if (isSemanticMode && currentQ) {
      // Semantic search
      const { data } = await api.get<
        PaginatedResponse<Listing> & { _parsed_filters?: ParsedFilters }
      >("/listings/semantic-search", {
        params: {
          q: currentQ,
          page: Number(searchParams.get("page")) || 1,
          limit: 12,
        },
      });
      if (data._parsed_filters) setParsedFilters(data._parsed_filters);
      return data;
    }

    // Standard search
    setParsedFilters(null);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const params: any = {
      page: Number(searchParams.get("page")) || 1,
      limit: 12,
    };
    if (currentQ) params.q = currentQ;
    const currentPropertyType = searchParams.get("property_type");
    if (currentPropertyType && currentPropertyType !== "all")
      params.property_type = currentPropertyType;
    const currentBuyRent = searchParams.get("buy_rent");
    if (currentBuyRent && currentBuyRent !== "all")
      params.buy_rent = currentBuyRent;
    const { data } = await api.get<PaginatedResponse<Listing>>("/listings/", {
      params,
    });
    return data;
  };

  const { data, isLoading, isError } = useQuery({
    queryKey: ["listings", searchParams.toString()],
    queryFn,
    placeholderData: (previousData) => previousData,
  });

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    updateFilters({ q, mode: aiMode ? "ai" : "" });
  };

  // Friendly label map for parsed filter tags
  const filterLabels: Record<string, string> = {
    min_price: "Min $",
    max_price: "Max $",
    beds: "Beds ≥",
    property_type: "Type",
    buy_rent: "Mode",
    tenure: "Tenure",
    district: "District",
    query: "Keyword",
  };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Navbar />

      <div className="container mx-auto px-4 py-8 flex-grow">
        {/* Search Bar */}
        <div className="mb-6 space-y-3">
          <div className="flex flex-col md:flex-row gap-3 items-end">
            {/* Search input */}
            <div className="grid w-full gap-1.5 md:max-w-lg">
              <Label htmlFor="search" className="flex items-center gap-2">
                Search
                {aiMode && (
                  <span className="inline-flex items-center gap-1 text-[11px] font-semibold px-2 py-0.5 rounded-full bg-violet-600 text-white">
                    <Sparkles className="h-3 w-3" /> AI
                  </span>
                )}
              </Label>
              <form onSubmit={handleSearch} className="flex gap-2">
                <Input
                  id="search"
                  placeholder={
                    aiMode
                      ? "e.g. 3BR condo Tampines 1.2m freehold..."
                      : "Search location, project..."
                  }
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                />
                <Button
                  type="submit"
                  className={aiMode ? "bg-violet-600 hover:bg-violet-700" : ""}
                >
                  {aiMode ? (
                    <Sparkles className="mr-1 h-4 w-4" />
                  ) : (
                    <Search className="mr-1 h-4 w-4" />
                  )}
                  Search
                </Button>
              </form>
            </div>

            {/* AI toggle */}
            <div className="flex items-center gap-2 pb-0.5">
              <button
                type="button"
                onClick={() => {
                  const next = !aiMode;
                  setAiMode(next);
                  setParsedFilters(null);
                  updateFilters({ mode: next ? "ai" : "" });
                }}
                className={`relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none ${aiMode ? "bg-violet-600" : "bg-gray-300"}`}
              >
                <span
                  className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${aiMode ? "translate-x-6" : "translate-x-1"}`}
                />
              </button>
              <span className="text-sm font-medium whitespace-nowrap flex items-center gap-1">
                <Sparkles className="h-3.5 w-3.5 text-violet-500" /> AI Search
              </span>
            </div>

            {/* Standard filters (hidden in AI mode) */}
            {!aiMode && (
              <>
                <div className="grid gap-1.5 min-w-[140px]">
                  <Label>Type</Label>
                  <Select
                    value={propertyType}
                    onValueChange={(val) => {
                      setPropertyType(val);
                      setPage(1);
                      updateFilters({ propertyType: val, page: 1 });
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="All Types" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Types</SelectItem>
                      <SelectItem value="Condo">Condo</SelectItem>
                      <SelectItem value="Landed">Landed</SelectItem>
                      <SelectItem value="HDB">HDB</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="grid gap-1.5 min-w-[140px]">
                  <Label>Mode</Label>
                  <Select
                    value={buyRent}
                    onValueChange={(val) => {
                      setBuyRent(val);
                      setPage(1);
                      updateFilters({ buyRent: val, page: 1 });
                    }}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="All Modes" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Modes</SelectItem>
                      <SelectItem value="Buy">Buy</SelectItem>
                      <SelectItem value="Rent">Rent</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </>
            )}
          </div>

          {/* Parsed filter tags (AI mode) */}
          {isSemanticMode &&
            parsedFilters &&
            Object.keys(parsedFilters).length > 0 && (
              <div className="flex flex-wrap gap-2 items-center pt-1">
                <span className="text-xs text-muted-foreground flex items-center gap-1">
                  <Sparkles className="h-3 w-3 text-violet-500" /> Understood
                  as:
                </span>
                {Object.entries(parsedFilters).map(([key, val]) =>
                  val != null ? (
                    <FilterTag
                      key={key}
                      label={filterLabels[key] ?? key}
                      value={val}
                    />
                  ) : null,
                )}
              </div>
            )}
        </div>

        {/* Results */}
        {isLoading && !data ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
            {Array.from({ length: 8 }).map((_, i) => (
              <div key={i} className="space-y-3">
                <Skeleton className="h-[200px] w-full rounded-xl" />
                <Skeleton className="h-4 w-3/4" />
              </div>
            ))}
          </div>
        ) : isError ? (
          <div className="text-center py-20 text-red-500">
            Failed to load listings. Please try again.
          </div>
        ) : (
          <div className="flex flex-col lg:flex-row gap-6 h-full">
            {/* Map View */}
            <div className="hidden lg:block w-1/3 h-[calc(100vh-200px)] sticky top-24 rounded-xl overflow-hidden border">
              <MapView
                listings={data?.data}
                center={DEFAULT_CENTER}
                zoom={11}
              />
            </div>

            {/* List View */}
            <div className="flex-1">
              <div className="flex justify-between items-center mb-4">
                <span className="text-muted-foreground">
                  {data?.total} results found
                </span>
                <Button
                  variant="outline"
                  className="lg:hidden"
                  onClick={() => alert("Mobile map view coming soon")}
                >
                  View Map
                </Button>
              </div>

              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
                {data?.data.map((listing) => (
                  <ListingCard key={listing.id} listing={listing} />
                ))}
              </div>

              {/* Pagination */}
              <div className="flex justify-center mt-12 gap-2 items-center flex-wrap">
                <Button
                  variant="outline"
                  size="icon"
                  disabled={page <= 1}
                  onClick={() => {
                    setPage(1);
                    updateFilters({ page: 1 });
                  }}
                  title="First Page"
                >
                  «
                </Button>
                <Button
                  variant="outline"
                  size="icon"
                  disabled={page <= 1}
                  onClick={() => {
                    const p = page - 1;
                    setPage(p);
                    updateFilters({ page: p });
                  }}
                  title="Previous Page"
                >
                  ‹
                </Button>

                {(() => {
                  const totalPages = data
                    ? Math.ceil(data.total / data.limit)
                    : 1;
                  const items: (number | string)[] = [];
                  items.push(1);
                  if (page > 3) items.push("...");
                  let start = Math.max(2, page - 1);
                  let end = Math.min(totalPages - 1, page + 1);
                  if (page < 4) {
                    end = Math.min(totalPages - 1, 4);
                    start = 2;
                  }
                  if (page > totalPages - 3) {
                    start = Math.max(2, totalPages - 3);
                    end = totalPages - 1;
                  }
                  for (let i = start; i <= end; i++) items.push(i);
                  if (page < totalPages - 2) items.push("...");
                  if (totalPages > 1) items.push(totalPages);
                  return items.map((item, idx) =>
                    item === "..." ? (
                      <span
                        key={`e-${idx}`}
                        className="px-2 text-muted-foreground"
                      >
                        ...
                      </span>
                    ) : (
                      <Button
                        key={item}
                        variant={item === page ? "default" : "outline"}
                        size="icon"
                        className="w-10"
                        onClick={() => {
                          setPage(item as number);
                          updateFilters({ page: item as number });
                        }}
                      >
                        {item}
                      </Button>
                    ),
                  );
                })()}

                <Button
                  variant="outline"
                  size="icon"
                  disabled={
                    data ? page >= Math.ceil(data.total / data.limit) : true
                  }
                  onClick={() => {
                    const p = page + 1;
                    setPage(p);
                    updateFilters({ page: p });
                  }}
                  title="Next Page"
                >
                  ›
                </Button>
                <Button
                  variant="outline"
                  size="icon"
                  disabled={
                    data ? page >= Math.ceil(data.total / data.limit) : true
                  }
                  onClick={() => {
                    const tp = data ? Math.ceil(data.total / data.limit) : 1;
                    setPage(tp);
                    updateFilters({ page: tp });
                  }}
                  title="Last Page"
                >
                  »
                </Button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default function ListingsPage() {
  return (
    <Suspense fallback={<div>Loading...</div>}>
      <ListingsContent />
    </Suspense>
  );
}
