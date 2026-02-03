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
  SelectValue 
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Label } from "@/components/ui/label";
import MapView from "@/components/features/map/map-view";

const DEFAULT_CENTER: [number, number] = [1.3521, 103.8198];

function ListingsContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  
  // State for filters (sync with URL initially)
  const [q, setQ] = useState(searchParams.get("q") || "");
  const [propertyType, setPropertyType] = useState(searchParams.get("property_type") || "all");
  const [buyRent, setBuyRent] = useState(searchParams.get("buy_rent") || "all");
  const [page, setPage] = useState(Number(searchParams.get("page")) || 1);
  
  // Sync state with URL params when they change (e.g. back button, deep link)
  useEffect(() => {
    const currentQ = searchParams.get("q") || "";
    const currentPropertyType = searchParams.get("property_type") || "all";
    const currentBuyRent = searchParams.get("buy_rent") || "all";
    const currentPage = Number(searchParams.get("page")) || 1;

    setQ(currentQ);
    setPropertyType(currentPropertyType);
    setBuyRent(currentBuyRent);
    setPage(currentPage);
  }, [searchParams]);
  
  // Debounce search/filter updates to URL
  const updateFilters = (overrides?: { 
      q?: string; 
      propertyType?: string; 
      buyRent?: string; 
      page?: number;
  }) => {
    const nextQ = overrides?.q ?? q;
    const nextPropertyType = overrides?.propertyType ?? propertyType;
    const nextBuyRent = overrides?.buyRent ?? buyRent;
    const nextPage = overrides?.page ?? page;

    const params = new URLSearchParams(searchParams.toString()); // Start with existing params
    
    if (nextQ) params.set("q", nextQ); else params.delete("q");
    if (nextPropertyType && nextPropertyType !== "all") params.set("property_type", nextPropertyType); else params.delete("property_type");
    if (nextBuyRent && nextBuyRent !== "all") params.set("buy_rent", nextBuyRent); else params.delete("buy_rent");
    
    // Always reset to page 1 on filter/map change unless explicit page change
    if (overrides?.page) {
        params.set("page", nextPage.toString());
    } else if (overrides) {
        // If any filter changed, reset page
        params.set("page", "1");
        setPage(1);
    }
    
    router.push(`/listings?${params.toString()}`);
  };

  // Convert current state to API params
  // Use searchParams as the source of truth for the API call to ensure consistency with URL
  const queryFn = async () => {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const params: any = { 
        page: Number(searchParams.get("page")) || 1, 
        limit: 12 
    };

    const currentQ = searchParams.get("q");
    if (currentQ) params.q = currentQ;

    const currentPropertyType = searchParams.get("property_type");
    if (currentPropertyType && currentPropertyType !== "all") params.property_type = currentPropertyType;

    const currentBuyRent = searchParams.get("buy_rent");
    if (currentBuyRent && currentBuyRent !== "all") params.buy_rent = currentBuyRent;

    const { data } = await api.get<PaginatedResponse<Listing>>("/listings/", { params });
    return data;
  };

  const { data, isLoading, isError } = useQuery({
    queryKey: ["listings", searchParams.toString()], // Use searchParams as main key
    queryFn: queryFn,
    placeholderData: (previousData) => previousData, // Keep showing previous data while fetching new
  });

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    updateFilters({ q });
  };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <Navbar />
      
      <div className="container mx-auto px-4 py-8 flex-grow">
        {/* Filters Header */}
        <div className="mb-8 space-y-4">
          <div className="flex flex-col md:flex-row gap-4 items-end">
             <div className="grid w-full gap-1.5 md:max-w-md">
                <Label htmlFor="search">Search</Label>
                <form onSubmit={handleSearch} className="flex gap-2">
                  <Input 
                    id="search" 
                    placeholder="Search location, project..." 
                    value={q} 
                    onChange={(e) => setQ(e.target.value)} 
                  />
                  <Button type="submit">Search</Button>
                </form>
             </div>
             
             <div className="grid gap-1.5 min-w-[140px]">
                <Label>Type</Label>
                <Select value={propertyType} onValueChange={(val) => { 
                    setPropertyType(val); 
                    setPage(1); 
                    updateFilters({ propertyType: val, page: 1 }); 
                }}>
                  <SelectTrigger><SelectValue placeholder="All Types" /></SelectTrigger>
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
                <Select value={buyRent} onValueChange={(val) => { 
                    setBuyRent(val); 
                    setPage(1); 
                    updateFilters({ buyRent: val, page: 1 }); 
                }}>
                  <SelectTrigger><SelectValue placeholder="All Modes" /></SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Modes</SelectItem>
                    <SelectItem value="Buy">Buy</SelectItem>
                    <SelectItem value="Rent">Rent</SelectItem>
                  </SelectContent>
                </Select>
             </div>
          </div>
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
          <div className="text-center py-20 text-red-500">Failed to load listings. Please try again.</div>
        ) : (
          <div className="flex flex-col lg:flex-row gap-6 h-full">
            {/* Map View - Desktop (Left) */}
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
                   <span className="text-muted-foreground">{data?.total} results found</span>
                   <Button variant="outline" className="lg:hidden" onClick={() => {
                       // Toggle mobile map view? For now just a placeholder action
                       alert("Mobile map view coming soon");
                   }}>View Map</Button>
                </div>
              
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-6">
                  {data?.data.map((listing) => (
                    <ListingCard key={listing.id} listing={listing} />
                  ))}
                </div>
    
                {/* Pagination (Advanced) */}
                <div className="flex justify-center mt-12 gap-2 items-center flex-wrap">
                   {/* First Page */}
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
                     <span className="sr-only">First</span>
                     «
                   </Button>

                   {/* Previous */}
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
                    <span className="sr-only">Previous</span>
                    ‹
                   </Button>
                   
                   {/* Page Numbers */}
                   {(() => {
                       const totalPages = data ? Math.ceil(data.total / data.limit) : 1;
                       const items = [];
                       
                       // Window size usually 1 or 2. User example '1412 1413 1414 1415' implies 2 or 3 neighbors.
                       // Let's use siblingCount = 1 for a balanced look, but special handling for start/end to show more.
                       
                       // Always show 1
                       items.push(1);
                       
                       if (page > 3) {
                           items.push("...");
                       }
                       
                       // Range around current
                       // We want to show at least one before and one after, but more if near edges
                       let start = Math.max(2, page - 1);
                       let end = Math.min(totalPages - 1, page + 1);

                       // Adjust for edges to keep constant number of items if possible?
                       // Simple heuristic:
                       if (page < 4) {
                           end = Math.min(totalPages - 1, 4); // Show 1 2 3 4 ...
                           start = 2;
                       }
                       if (page > totalPages - 3) {
                           start = Math.max(2, totalPages - 3); // ... 97 98 99 100
                           end = totalPages - 1;
                       }

                       for (let i = start; i <= end; i++) {
                           items.push(i);
                       }
                       
                       if (page < totalPages - 2) {
                           items.push("...");
                       }
                       
                       // Always show last
                       if (totalPages > 1) {
                           items.push(totalPages);
                       }

                       return items.map((item, idx) => {
                           if (item === "...") {
                               return <span key={`ellipsis-${idx}`} className="px-2 text-muted-foreground">...</span>;
                           }
                           const p = item as number;
                           return (
                               <Button
                                   key={p}
                                   variant={p === page ? "default" : "outline"}
                                   size="icon" // Using icon size for page numbers to keep them square-ish
                                   className="w-10" // Force a bit more width than strict icon
                                   onClick={() => {
                                       setPage(p);
                                       updateFilters({ page: p });
                                   }}
                               >
                                   {p}
                               </Button>
                           );
                       });
                   })()}
                   
                   {/* Next */}
                   <Button 
                     variant="outline" 
                     size="icon"
                     disabled={data ? page >= Math.ceil(data.total / data.limit) : true} 
                     onClick={() => { 
                         const p = page + 1;
                         setPage(p); 
                         updateFilters({ page: p }); 
                     }}
                     title="Next Page"
                   >
                     <span className="sr-only">Next</span>
                     ›
                   </Button>
                   
                   {/* Last Page */}
                   <Button
                     variant="outline"
                     size="icon"
                      disabled={data ? page >= Math.ceil(data.total / data.limit) : true}
                     onClick={() => {
                         const totalPages = data ? Math.ceil(data.total / data.limit) : 1;
                         setPage(totalPages);
                         updateFilters({ page: totalPages });
                     }}
                     title="Last Page"
                   >
                     <span className="sr-only">Last</span>
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
