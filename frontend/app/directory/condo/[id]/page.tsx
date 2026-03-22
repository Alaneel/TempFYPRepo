"use client";

import { useQuery } from "@tanstack/react-query";
import api from "@/lib/api";
import { useParams } from "next/navigation";
import { Navbar } from "@/components/layout/navbar";
import { ListingCard } from "@/components/features/listings/listing-card";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Building2, MapPin, Calendar, Ruler, Info } from "lucide-react";
import { UnitList } from "@/components/features/directory/unit-list";

export default function CondoDetailPage() {
    const params = useParams();
    const id = params.id as string;

    const { data: condo, isLoading: isLoadingCondo } = useQuery({
        queryKey: ["condo", id],
        queryFn: async () => {
            const res = await api.get(`/directory/condos/${id}`);
            return res.data;
        },
        enabled: !!id,
    });

    const { data: listingsData, isLoading: isLoadingListings } = useQuery({
        queryKey: ["condo_listings", id],
        queryFn: async () => {
            const res = await api.get(`/directory/condos/${id}/listings`, { params: { limit: 20 } });
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
                        {isLoadingCondo ? (
                            <div className="space-y-4">
                                <Skeleton className="h-10 w-1/3" />
                                <Skeleton className="h-6 w-1/4" />
                                <div className="flex gap-4 pt-4">
                                    <Skeleton className="h-20 w-32 rounded-lg" />
                                    <Skeleton className="h-20 w-32 rounded-lg" />
                                </div>
                            </div>
                        ) : condo ? (
                            <div>
                                <div className="flex items-center gap-3 mb-4 text-primary">
                                    <Building2 className="h-8 w-8" />
                                    <h1 className="text-4xl font-bold tracking-tight">{condo.condo_name}</h1>
                                </div>
                                <div className="flex items-center text-lg text-muted-foreground mb-8">
                                    <MapPin className="h-5 w-5 mr-2" />
                                    <span>{condo.street_name || "Unknown address"} • District {condo.district}</span>
                                </div>

                                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <Building2 className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">Property Type</span>
                                        <span className="font-semibold">{condo.property_type || "Condominium"}</span>
                                    </div>
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <Ruler className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">Total Units</span>
                                        <span className="font-semibold">{condo.total_units || "Unknown"}</span>
                                    </div>
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <Calendar className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">TOP / Built</span>
                                        <span className="font-semibold">{condo.top_date || condo.year_completed || "Unknown"}</span>
                                    </div>
                                    <div className="bg-card p-4 rounded-xl border shadow-sm flex flex-col items-center justify-center text-center">
                                        <Info className="h-6 w-6 text-muted-foreground mb-2" />
                                        <span className="text-sm font-medium text-muted-foreground">Tenure</span>
                                        <span className="font-semibold">{condo.tenure || "Unknown"}</span>
                                    </div>
                                </div>

                                {condo.developer_name && (
                                    <div className="mt-8 text-sm text-muted-foreground">
                                        Developed by: <span className="font-medium text-foreground">{condo.developer_name}</span>
                                    </div>
                                )}
                            </div>
                        ) : (
                            <div className="text-destructive">Condo not found.</div>
                        )}
                    </div>
                </section>

                {/* Units Directory Section (Permanent) */}
                <section className="container mx-auto px-4 py-12 max-w-5xl border-t">
                    <div className="flex items-center gap-2 mb-8">
                        <Building2 className="h-6 w-6 text-primary" />
                        <h2 className="text-3xl font-bold tracking-tight">Master Property Directory</h2>
                        <Badge variant="secondary" className="ml-2">Permanent Records</Badge>
                    </div>
                    <UnitList type="condo" id={id} />
                </section>

                {/* Live Listings Section (Temporary/Active) */}
                <section className="container mx-auto px-4 py-12 max-w-5xl border-t bg-muted/5">
                    <div className="flex items-center gap-2 mb-8">
                        <Info className="h-6 w-6 text-orange-500" />
                        <h2 className="text-3xl font-bold tracking-tight">Current Market Activity</h2>
                        <Badge variant="outline" className="ml-2 border-orange-200 text-orange-700 bg-orange-50">Active Listings</Badge>
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
                            <p className="text-muted-foreground text-lg">No active listings available for this property right now.</p>
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
