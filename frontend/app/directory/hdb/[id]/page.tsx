"use client";

import { useQuery } from "@tanstack/react-query";
import api from "@/lib/api";
import { useParams } from "next/navigation";
import { Navbar } from "@/components/layout/navbar";
import { ListingCard } from "@/components/features/listings/listing-card";
import { Skeleton } from "@/components/ui/skeleton";
import { Building, MapPin, Calendar, Ruler, CheckCircle2, Home } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { UnitList } from "@/components/features/directory/unit-list";

export default function HdbDetailPage() {
    const params = useParams();
    const id = params.id as string;

    const { data: hdb, isLoading: isLoadingHdb } = useQuery({
        queryKey: ["hdb", id],
        queryFn: async () => {
            const res = await api.get(`/directory/hdbs/${id}`);
            return res.data;
        },
        enabled: !!id,
    });

    const { data: listingsData, isLoading: isLoadingListings } = useQuery({
        queryKey: ["hdb_listings", id],
        queryFn: async () => {
            const res = await api.get(`/directory/hdbs/${id}/listings`, { params: { limit: 20 } });
            return res.data;
        },
        enabled: !!id,
    });

    return (
        <div className="min-h-screen flex flex-col bg-background">
            <Navbar />

            <main className="flex-grow">
                {/* Header Section */}
                <section className="bg-muted/30 border-b">
                    <div className="container mx-auto px-4 py-12 max-w-5xl">
                        {isLoadingHdb ? (
                            <div className="space-y-4">
                                <Skeleton className="h-10 w-1/3" />
                                <Skeleton className="h-6 w-1/4" />
                                <div className="flex gap-4 pt-4">
                                    <Skeleton className="h-20 w-32 rounded-lg" />
                                    <Skeleton className="h-20 w-32 rounded-lg" />
                                </div>
                            </div>
                        ) : hdb ? (
                            <div>
                                <div className="flex items-center gap-3 mb-4 text-primary">
                                    <Building className="h-8 w-8" />
                                    <h1 className="text-4xl font-bold tracking-tight">Block {hdb.block_number}</h1>
                                    <Badge variant="outline" className="ml-2 bg-background">HDB</Badge>
                                </div>
                                <div className="flex items-center text-lg text-muted-foreground mb-8">
                                    <MapPin className="h-5 w-5 mr-2" />
                                    <span>{hdb.street_name || "Unknown street"} • {hdb.town}</span>
                                </div>

                                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <Home className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">Dwelling Units</span>
                                        <span className="font-semibold">{hdb.total_dwelling_units || hdb.total_units || "Unknown"}</span>
                                    </div>
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <Ruler className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">Highest Floor</span>
                                        <span className="font-semibold">{hdb.total_floors || "Unknown"}</span>
                                    </div>
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <Calendar className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">Built Year</span>
                                        <span className="font-semibold">{hdb.year_completed || "Unknown"}</span>
                                    </div>
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <CheckCircle2 className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">Commercial</span>
                                        <span className="font-semibold">{hdb.has_commercial ? "Yes" : "No"}</span>
                                    </div>
                                </div>

                                <div className="bg-background border rounded-xl p-6">
                                    <h3 className="text-lg font-bold mb-4">Block Facilities & Unit Mix</h3>
                                    <div className="grid grid-cols-2 md:grid-cols-3 gap-y-4 text-sm">
                                        <div className="flex items-center gap-2">
                                            <span className="text-muted-foreground">Void Deck:</span>
                                            <span className="font-medium">{hdb.has_void_deck ? "Yes" : "No"}</span>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <span className="text-muted-foreground">Multistorey Carpark:</span>
                                            <span className="font-medium">{hdb.has_multistorey_carpark ? "Yes" : "No"}</span>
                                        </div>
                                        <div className="flex items-center gap-2">
                                            <span className="text-muted-foreground">Market / Hawker:</span>
                                            <span className="font-medium">{hdb.has_market_hawker ? "Yes" : "No"}</span>
                                        </div>
                                        {/* Unit Mix distribution if data exists */}
                                        {hdb.three_room_qty > 0 && (
                                            <div className="flex items-center gap-2">
                                                <span className="text-muted-foreground">3-Room Units:</span>
                                                <span className="font-medium">{hdb.three_room_qty}</span>
                                            </div>
                                        )}
                                        {hdb.four_room_qty > 0 && (
                                            <div className="flex items-center gap-2">
                                                <span className="text-muted-foreground">4-Room Units:</span>
                                                <span className="font-medium">{hdb.four_room_qty}</span>
                                            </div>
                                        )}
                                        {hdb.five_room_qty > 0 && (
                                            <div className="flex items-center gap-2">
                                                <span className="text-muted-foreground">5-Room Units:</span>
                                                <span className="font-medium">{hdb.five_room_qty}</span>
                                            </div>
                                        )}
                                    </div>
                                </div>

                            </div>
                        ) : (
                            <div className="text-destructive">HDB Block not found.</div>
                        )}
                    </div>
                </section>

                {/* Units Directory Section (Permanent) */}
                <section className="container mx-auto px-4 py-12 max-w-5xl border-t">
                    <div className="flex items-center gap-2 mb-8">
                        <Home className="h-6 w-6 text-primary" />
                        <h2 className="text-3xl font-bold tracking-tight">Master Block Directory</h2>
                        <Badge variant="secondary" className="ml-2">Permanent Records</Badge>
                    </div>
                    <UnitList type="hdb" id={id} />
                </section>

                {/* Live Listings Section (Temporary/Active) */}
                <section className="container mx-auto px-4 py-12 max-w-5xl border-t bg-muted/5">
                    <div className="flex items-center gap-2 mb-8">
                        <CheckCircle2 className="h-6 w-6 text-orange-500" />
                        <h2 className="text-3xl font-bold tracking-tight">Active Market Status</h2>
                        <Badge variant="outline" className="ml-2 border-orange-200 text-orange-700 bg-orange-50">Live Listings</Badge>
                    </div>

                    {isLoadingListings ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                            {[1, 2, 3].map(i => <Skeleton key={i} className="h-80 w-full rounded-xl" />)}
                        </div>
                    ) : listingsData?.data?.length > 0 ? (
                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                            {listingsData.data.map((listing: any) => (
                                <ListingCard key={listing.id} listing={listing} />
                            ))}
                        </div>
                    ) : (
                        <div className="text-center py-20 border-2 border-dashed rounded-xl bg-muted/10">
                            <p className="text-muted-foreground text-lg">No active listings available for this block right now.</p>
                        </div>
                    )}
                </section>
            </main>

            <footer className="border-t py-8 bg-muted/30 mt-auto text-center text-sm text-muted-foreground">
                <p>© 2026 SgEstate Directory.</p>
            </footer>
        </div>
    );
}
