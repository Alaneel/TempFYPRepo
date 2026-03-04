"use client";

import { useQuery } from "@tanstack/react-query";
import { useSearchParams, useRouter } from "next/navigation";
import { useState, useEffect, Suspense, useCallback } from "react";
import api from "@/lib/api";
import { Listing, PaginatedResponse } from "@/types";
import { ListingCard } from "@/components/features/listings/listing-card";
import { FilterModal, FilterValues } from "@/components/features/listings/filter-modal";
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
import { Sparkles, Search, X, SlidersHorizontal } from "lucide-react";

const DEFAULT_CENTER: [number, number] = [1.3521, 103.8198];

type ParsedFilters = Record<string, string | number | null>;

const AI_EXAMPLES = [
  "3BR freehold condo near Tampines MRT under $1.5m",
  "HDB 4-room flat for rent in Bishan below $3000/mo",
  "Landed bungalow in Bukit Timah freehold above $5m",
];

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
  const [page, setPage] = useState(Number(searchParams.get("page")) || 1);
  const [sortBy, setSortBy] = useState(searchParams.get("sort_by") || "recommended");
  const [filterOpen, setFilterOpen] = useState(false);

  // ── Map bbox state ─────────────────────────────────────────────
  const [mapBounds, setMapBounds] = useState<{ minLat: number; maxLat: number; minLng: number; maxLng: number } | null>(null);
  const [mapFilterActive, setMapFilterActive] = useState(false);
  const [activeMapId, setActiveMapId] = useState<number | null>(null);

  const handleBoundsChange = useCallback((bounds: { minLat: number; maxLat: number; minLng: number; maxLng: number }) => {
    setMapBounds(bounds);
  }, []);

  // ── Filter values (all synced to URL) ─────────────────────────
  const [filterValues, setFilterValues] = useState<FilterValues>({
    propertyType: searchParams.get("property_type") || "all",
    buyRent: searchParams.get("buy_rent") || "all",
    minPrice: searchParams.get("min_price") || "",
    maxPrice: searchParams.get("max_price") || "",
    beds: searchParams.get("beds") || "any",
  });

  // Parsed filters returned by semantic search (for display)
  const [parsedFilters, setParsedFilters] = useState<ParsedFilters | null>(null);
  // Fallback notice when AI search relaxed filters to find results
  const [fallbackNotice, setFallbackNotice] = useState<string | null>(null);

  // Sync state with URL params
  useEffect(() => {
    setQ(searchParams.get("q") || "");
    setPage(Number(searchParams.get("page")) || 1);
    setAiMode(searchParams.get("mode") === "ai");
    setSortBy(searchParams.get("sort_by") || "recommended");
    setFilterValues({
      propertyType: searchParams.get("property_type") || "all",
      buyRent: searchParams.get("buy_rent") || "all",
      minPrice: searchParams.get("min_price") || "",
      maxPrice: searchParams.get("max_price") || "",
      beds: searchParams.get("beds") || "any",
    });
  }, [searchParams]);

  const buildParams = (overrides?: {
    q?: string;
    page?: number;
    mode?: string;
    sortBy?: string;
    filters?: FilterValues;
  }) => {
    const fv = overrides?.filters ?? filterValues;
    const nextQ = overrides?.q ?? q;
    const nextPage = overrides?.page ?? page;
    const nextMode = overrides?.mode ?? (aiMode ? "ai" : "");
    const nextSortBy = overrides?.sortBy ?? sortBy;

    const params = new URLSearchParams();
    if (nextQ) params.set("q", nextQ);
    if (nextMode) params.set("mode", nextMode);
    if (nextSortBy && nextSortBy !== "recommended") params.set("sort_by", nextSortBy);
    if (nextPage > 1) params.set("page", nextPage.toString());
    if (fv.propertyType && fv.propertyType !== "all") params.set("property_type", fv.propertyType);
    if (fv.buyRent && fv.buyRent !== "all") params.set("buy_rent", fv.buyRent);
    if (fv.minPrice) params.set("min_price", fv.minPrice);
    if (fv.maxPrice) params.set("max_price", fv.maxPrice);
    if (fv.beds && fv.beds !== "any" && fv.beds !== "") params.set("beds", fv.beds);
    return params;
  };

  const updateFilters = (overrides?: Parameters<typeof buildParams>[0]) => {
    const params = buildParams(overrides);
    router.push(`/listings?${params.toString()}`);
  };

  const handleApplyFilters = (values: FilterValues) => {
    setFilterValues(values);
    const params = buildParams({ filters: values, page: 1 });
    router.push(`/listings?${params.toString()}`);
  };

  // Active filter count badge
  const activeFilterCount = [
    filterValues.propertyType !== "all",
    filterValues.buyRent !== "all",
    filterValues.minPrice !== "",
    filterValues.maxPrice !== "",
    filterValues.beds !== "any" && filterValues.beds !== "",
  ].filter(Boolean).length;

  // ── API call ──────────────────────────────────────────────────
  const isSemanticMode = searchParams.get("mode") === "ai";
  const currentQ = searchParams.get("q") || "";

  const queryFn = async () => {
    if (isSemanticMode && currentQ) {
      const { data } = await api.get<
        PaginatedResponse<Listing> & { _parsed_filters?: ParsedFilters; _fallback_notice?: string }
      >("/listings/semantic-search", {
        params: {
          q: currentQ,
          page: Number(searchParams.get("page")) || 1,
          limit: 12,
          sort_by: searchParams.get("sort_by") || "recommended",
        },
      });
      if (data._parsed_filters) setParsedFilters(data._parsed_filters);
      setFallbackNotice(data._fallback_notice ?? null);
      return data;
    }

    setParsedFilters(null);
    setFallbackNotice(null);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const params: any = {
      page: Number(searchParams.get("page")) || 1,
      limit: 12,
    };
    if (currentQ) params.q = currentQ;
    const pt = searchParams.get("property_type");
    if (pt && pt !== "all") params.property_type = pt;
    const br = searchParams.get("buy_rent");
    if (br && br !== "all") params.buy_rent = br;
    const minP = searchParams.get("min_price");
    if (minP) params.min_price = Number(minP);
    const maxP = searchParams.get("max_price");
    if (maxP) params.max_price = Number(maxP);
    const beds = searchParams.get("beds");
    if (beds && beds !== "any") params.beds = beds === "Studio" ? 0 : Number(beds.replace("+", ""));
    const sb = searchParams.get("sort_by");
    if (sb && sb !== "recommended") params.sort_by = sb;

    // bbox 过滤
    if (mapFilterActive && mapBounds) {
      params.min_lat = mapBounds.minLat;
      params.max_lat = mapBounds.maxLat;
      params.min_lng = mapBounds.minLng;
      params.max_lng = mapBounds.maxLng;
    }

    const { data } = await api.get<PaginatedResponse<Listing>>("/listings/", { params });
    return data;
  };

  const { data, isLoading, isError } = useQuery({
    queryKey: ["listings", searchParams.toString(), mapFilterActive ? mapBounds : null],
    queryFn,
    placeholderData: (previousData) => previousData,
  });

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    updateFilters({ q, mode: aiMode ? "ai" : "", page: 1 });
  };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Navbar />

      <FilterModal
        open={filterOpen}
        onClose={() => setFilterOpen(false)}
        values={filterValues}
        onApply={handleApplyFilters}
      />

      <div className="container mx-auto px-4 sm:px-6 lg:px-8 py-8 flex-grow">
        {/* Search & Filter Bar */}
        <div className="mb-8 space-y-3">
          <div className="bg-white border rounded-2xl p-4 shadow-sm flex flex-col md:flex-row gap-4 items-center justify-between">
            {/* Search input */}
            <div className="w-full flex-1 max-w-2xl">
              <Label htmlFor="search" className="sr-only">Search</Label>
              <form onSubmit={handleSearch} className="flex gap-2 w-full">
                <div className="relative flex items-center w-full">
                  {!aiMode && <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />}
                  {aiMode && <Sparkles className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-violet-500" />}
                  <Input
                    id="search"
                    className="pl-10 h-10 text-base rounded-xl border-gray-300 focus-visible:ring-violet-500/50 w-full"
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
                    className={`absolute right-1 top-1 h-8 rounded-lg px-4 ${aiMode ? "bg-violet-600 hover:bg-violet-700" : ""}`}
                  >
                    Search
                  </Button>
                </div>
              </form>
            </div>

            {/* Right controls */}
            <div className="flex flex-wrap items-center gap-3 w-full md:w-auto justify-start md:justify-end shrink-0">
              {/* AI toggle */}
              <div className="flex items-center gap-2 bg-gray-50 px-3 h-10 rounded-xl border shrink-0">
                <button
                  type="button"
                  onClick={() => {
                    const next = !aiMode;
                    setAiMode(next);
                    setParsedFilters(null);
                    updateFilters({ mode: next ? "ai" : "" });
                  }}
                  className={`relative inline-flex h-6 w-11 shrink-0 items-center rounded-full transition-colors focus:outline-none ${aiMode ? "bg-violet-600" : "bg-gray-300"}`}
                >
                  <span
                    className={`inline-block h-4 w-4 transform rounded-full bg-white shadow transition-transform ${aiMode ? "translate-x-6" : "translate-x-1"}`}
                  />
                </button>
                <span className="text-sm font-semibold whitespace-nowrap flex items-center gap-1.5">
                  <Sparkles className="h-4 w-4 text-violet-500" /> AI Search
                </span>
              </div>

              {/* Filters button (hidden in AI mode) */}
              {!aiMode && (
                <Button
                  variant="outline"
                  className="h-10 rounded-xl gap-2 relative"
                  onClick={() => setFilterOpen(true)}
                >
                  <SlidersHorizontal className="h-4 w-4" />
                  Filters
                  {activeFilterCount > 0 && (
                    <span className="absolute -top-1.5 -right-1.5 h-5 w-5 rounded-full bg-primary text-primary-foreground text-[10px] font-bold flex items-center justify-center">
                      {activeFilterCount}
                    </span>
                  )}
                </Button>
              )}
            </div>
          </div>

          {/* AI example prompts */}
          {aiMode && !currentQ && (
            <div className="flex flex-wrap gap-2 items-center">
              <span className="text-xs text-muted-foreground">Try:</span>
              {AI_EXAMPLES.map((ex) => (
                <button
                  key={ex}
                  onClick={() => {
                    setQ(ex);
                    updateFilters({ q: ex, mode: "ai", page: 1 });
                  }}
                  className="text-xs px-3 py-1.5 rounded-full border border-violet-200 bg-violet-50 text-violet-700 hover:bg-violet-100 transition-colors"
                >
                  {ex}
                </button>
              ))}
            </div>
          )}

          {/* Active filter tags summary (standard mode) */}
          {!aiMode && activeFilterCount > 0 && (
            <div className="flex flex-wrap gap-2 items-center">
              {filterValues.propertyType !== "all" && (
                <FilterTag label="Type" value={filterValues.propertyType} onRemove={() => handleApplyFilters({ ...filterValues, propertyType: "all" })} />
              )}
              {filterValues.buyRent !== "all" && (
                <FilterTag label="Mode" value={filterValues.buyRent === "sale" ? "Buy" : "Rent"} onRemove={() => handleApplyFilters({ ...filterValues, buyRent: "all" })} />
              )}
              {filterValues.minPrice && (
                <FilterTag label="Min $" value={`$${Number(filterValues.minPrice).toLocaleString()}`} onRemove={() => handleApplyFilters({ ...filterValues, minPrice: "" })} />
              )}
              {filterValues.maxPrice && (
                <FilterTag label="Max $" value={`$${Number(filterValues.maxPrice).toLocaleString()}`} onRemove={() => handleApplyFilters({ ...filterValues, maxPrice: "" })} />
              )}
              {filterValues.beds && filterValues.beds !== "any" && (
                <FilterTag label="Beds" value={filterValues.beds === "Studio" ? "Studio" : `${filterValues.beds}+`} onRemove={() => handleApplyFilters({ ...filterValues, beds: "any" })} />
              )}
            </div>
          )}

          {/* AI 解析摘要横幅 */}
          {isSemanticMode && parsedFilters && Object.keys(parsedFilters).length > 0 && (() => {
            // 将 parsedFilters 转成人类可读的摘要短语
            const f = parsedFilters;
            const chips: string[] = [];

            // beds
            if (f.beds != null) chips.push(`${f.beds}BR`);
            // property_type
            if (f.property_type) chips.push(String(f.property_type));
            // buy/rent
            if (f.buy_rent) chips.push(String(f.buy_rent) === "property-for-rent" ? "For Rent" : "For Sale");
            // tenure
            if (f.tenure) chips.push(String(f.tenure));
            // districts array (multi-district "near X" support)
            if (Array.isArray(f.districts) && (f.districts as number[]).length > 0) {
              const ds = (f.districts as number[]).map(d => `D${d}`).join("/");
              chips.push(ds);
            } else if (f.district != null) {
              chips.push(`District ${f.district}`);
            }
            // free-text location keyword
            if (f.query) chips.push(String(f.query));
            // price range
            const fmt$ = (n: number) => n >= 1_000_000 ? `$${(n/1_000_000).toFixed(1)}M` : `$${(n/1000).toFixed(0)}K`;
            if (f.min_price != null && f.max_price != null)
              chips.push(`${fmt$(Number(f.min_price))}–${fmt$(Number(f.max_price))}`);
            else if (f.max_price != null) chips.push(`Under ${fmt$(Number(f.max_price))}`);
            else if (f.min_price != null) chips.push(`Above ${fmt$(Number(f.min_price))}`);

            return (
              <div className="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-violet-50 border border-violet-200 text-sm">
                <Sparkles className="h-4 w-4 text-violet-500 flex-shrink-0" />
                <span className="text-violet-600 font-semibold mr-1">AI understood:</span>
                <span className="text-violet-900 font-medium">
                  {chips.join(" · ")}
                </span>
              </div>
            );
          })()}

          {/* Fallback notice — shown when AI relaxed filters to find results */}
          {isSemanticMode && fallbackNotice && (
            <div className="flex items-center gap-2 px-4 py-2.5 rounded-xl bg-amber-50 border border-amber-200 text-sm text-amber-800">
              <span className="text-lg leading-none">⚠️</span>
              <span>
                <span className="font-semibold">No exact matches found.</span>{" "}
                {fallbackNotice.charAt(0).toUpperCase() + fallbackNotice.slice(1)}.
              </span>
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
          <div className="flex flex-col lg:flex-row gap-8 h-full items-start">
            {/* Map View */}
            <div className="hidden lg:flex lg:flex-col w-1/3 h-[calc(100vh-140px)] sticky top-[100px] gap-2 z-10">
              <div className="rounded-2xl overflow-hidden border shadow-sm flex-1">
                <MapView
                  listings={data?.data}
                  center={DEFAULT_CENTER}
                  zoom={11}
                  onBoundsChange={handleBoundsChange}
                  onActiveChange={setActiveMapId}
                />
              </div>
              {/* 在地图范围内搜索 按钮 */}
              <button
                onClick={() => {
                  const next = !mapFilterActive;
                  setMapFilterActive(next);
                  setPage(1);
                  // 开启地图筛选时清空关键词，避免 bbox+query AND 导致 0 结果
                  if (next && q) {
                    setQ("");
                    updateFilters({ q: "", page: 1 });
                  }
                }}
                className={`w-full py-2 px-4 rounded-xl text-sm font-semibold border transition-colors ${
                  mapFilterActive
                    ? "bg-slate-900 text-white border-slate-900"
                    : "bg-white text-slate-700 border-slate-300 hover:border-slate-500"
                }`}
              >
                {mapFilterActive ? (
                    <>✓ 只显示地图范围内的房源{data?.total != null ? <span className="ml-1.5 font-normal opacity-80">· {data.total.toLocaleString()} 个</span> : null}</>
                  ) : (
                    <>在地图范围内搜索{q ? <span className="ml-1.5 font-normal opacity-60">（将清空关键词）</span> : null}</>
                  )}
              </button>
            </div>

            {/* List View */}
            <div className="flex-1 w-full lg:w-2/3">
              <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center mb-6 pb-4 border-b">
                <h2 className="text-lg font-semibold text-foreground flex items-center gap-2">
                  <span className="text-muted-foreground font-normal">Showing</span>
                  {data?.total.toLocaleString()} results
                </h2>

                <div className="flex items-center gap-3 mt-4 sm:mt-0">
                  <Select
                    value={sortBy}
                    onValueChange={(val) => {
                      setSortBy(val);
                      setPage(1);
                      updateFilters({ sortBy: val, page: 1 });
                    }}
                  >
                    <SelectTrigger className="w-[160px] h-9">
                      <SelectValue placeholder="Sort by" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="recommended">Recommended</SelectItem>
                      <SelectItem value="price_asc">Price: Low to High</SelectItem>
                      <SelectItem value="price_desc">Price: High to Low</SelectItem>
                      <SelectItem value="newest">Newest First</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 xl:grid-cols-3 gap-6">
                {data?.data.map((listing) => (
                  <ListingCard key={listing.id} listing={listing} isHighlighted={activeMapId === listing.id} />
                ))}
              </div>

              {/* 0 结果提示 */}
              {!isLoading && data?.total === 0 && (
                <div className="flex flex-col items-center justify-center py-20 gap-4 text-center">
                  <div className="text-5xl">🔍</div>
                  <h3 className="text-lg font-semibold text-foreground">没有找到符合条件的房源</h3>
                  {currentQ && !isSemanticMode ? (
                    <>
                      <p className="text-muted-foreground max-w-sm">
                        普通搜索需要关键词完整出现在标题或地址中。试试 <span className="font-semibold text-violet-600">AI Search</span>，用自然语言描述你的需求。
                      </p>
                      <button
                        onClick={() => {
                          setAiMode(true);
                          updateFilters({ mode: "ai", q, page: 1 });
                        }}
                        className="inline-flex items-center gap-2 px-5 py-2.5 rounded-xl bg-violet-600 hover:bg-violet-700 text-white text-sm font-semibold transition-colors"
                      >
                        <Sparkles className="h-4 w-4" />
                        用 AI Search 重试
                      </button>
                    </>
                  ) : (
                    <p className="text-muted-foreground max-w-sm">试试放宽筛选条件，或更换关键词。</p>
                  )}
                </div>
              )}

              {/* Pagination — 地图筛选模式下隐藏（结果已由地图范围决定，无需翻页） */}
              {!mapFilterActive && (
              <div className="flex justify-center mt-12 gap-2 items-center flex-wrap">
                <Button
                  variant="outline"
                  size="icon"
                  disabled={page <= 1}
                  onClick={() => { setPage(1); updateFilters({ page: 1 }); }}
                  title="First Page"
                >«</Button>
                <Button
                  variant="outline"
                  size="icon"
                  disabled={page <= 1}
                  onClick={() => { const p = page - 1; setPage(p); updateFilters({ page: p }); }}
                  title="Previous Page"
                >‹</Button>

                {(() => {
                  const totalPages = data ? Math.ceil(data.total / data.limit) : 1;
                  const items: (number | string)[] = [];
                  items.push(1);
                  if (page > 3) items.push("...");
                  let start = Math.max(2, page - 1);
                  let end = Math.min(totalPages - 1, page + 1);
                  if (page < 4) { end = Math.min(totalPages - 1, 4); start = 2; }
                  if (page > totalPages - 3) { start = Math.max(2, totalPages - 3); end = totalPages - 1; }
                  for (let i = start; i <= end; i++) items.push(i);
                  if (page < totalPages - 2) items.push("...");
                  if (totalPages > 1) items.push(totalPages);
                  return items.map((item, idx) =>
                    item === "..." ? (
                      <span key={`e-${idx}`} className="px-2 text-muted-foreground">...</span>
                    ) : (
                      <Button
                        key={item}
                        variant={item === page ? "default" : "outline"}
                        size="icon"
                        className="w-10"
                        onClick={() => { setPage(item as number); updateFilters({ page: item as number }); }}
                      >{item}</Button>
                    ),
                  );
                })()}

                <Button
                  variant="outline"
                  size="icon"
                  disabled={data ? page >= Math.ceil(data.total / data.limit) : true}
                  onClick={() => { const p = page + 1; setPage(p); updateFilters({ page: p }); }}
                  title="Next Page"
                >›</Button>
                <Button
                  variant="outline"
                  size="icon"
                  disabled={data ? page >= Math.ceil(data.total / data.limit) : true}
                  onClick={() => { const tp = data ? Math.ceil(data.total / data.limit) : 1; setPage(tp); updateFilters({ page: tp }); }}
                  title="Last Page"
                >»</Button>
              </div>
              )}
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
