"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import api from "@/lib/api";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Navbar } from "@/components/layout/navbar";
import { Search, MapPin, Building2, Building } from "lucide-react";
import Link from "next/link";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";

interface CondoResponse {
    id: number;
    condo_name: string;
    street_name: string;
    district: number;
    total_units: number;
    property_type: string;
}

interface HdbResponse {
    hdb_id: number;
    block_number: string;
    street_name: string;
    town: string;
    total_dwelling_units: number;
}

interface SearchResults {
    condos: CondoResponse[];
    hdbs: HdbResponse[];
}

export default function DirectorySearchPage() {
    const [query, setQuery] = useState("");
    const [activeQuery, setActiveQuery] = useState("");

    const { data, isLoading, isError } = useQuery<SearchResults>({
        queryKey: ["directorySearch", activeQuery],
        queryFn: async () => {
            if (!activeQuery) return { condos: [], hdbs: [] };
            const res = await api.get(`/directory/search`, { params: { q: activeQuery, limit: 30 } });
            return res.data;
        },
        enabled: activeQuery.length > 0,
    });

    const handleSearch = (e: React.FormEvent) => {
        e.preventDefault();
        setActiveQuery(query);
    };

    return (
        <div className="min-h-screen flex flex-col bg-background">
            <Navbar />

            <main className="flex-grow">
                {/* Hero Section */}
                <div className="bg-slate-900 overflow-hidden relative py-20 mb-12">
                    <div className="absolute inset-0 opacity-20 bg-[url('/placeholders/city_grid.png')] bg-cover bg-center" />
                    <div className="absolute inset-0 bg-gradient-to-b from-transparent to-background" />

                    <div className="container mx-auto px-4 max-w-5xl relative z-10 text-center">
                        <Badge className="mb-4 bg-primary/20 text-primary border-primary/30 hover:bg-primary/20">Master Database</Badge>
                        <h1 className="text-4xl md:text-5xl font-extrabold tracking-tight mb-4 text-white">
                            Property Directory
                        </h1>
                        <p className="text-xl text-slate-300 mb-10 max-w-2xl mx-auto">
                            The definitive database of every HDB block and Condominium in Singapore.
                        </p>

                        <form onSubmit={handleSearch} className="max-w-2xl mx-auto flex flex-col sm:flex-row gap-3">
                            <div className="relative flex-grow">
                                <Search className="absolute left-4 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                                <Input
                                    placeholder="Search project name, block, or street..."
                                    className="pl-12 h-14 text-lg bg-white/95 border-none shadow-2xl focus-visible:ring-primary"
                                    value={query}
                                    onChange={(e) => setQuery(e.target.value)}
                                />
                            </div>
                            <Button type="submit" size="lg" className="h-14 px-8 text-lg font-bold shadow-xl">
                                Search Directory
                            </Button>
                        </form>
                    </div>
                </div>

                <div className="container mx-auto px-4 max-w-5xl pb-20">

                    {activeQuery && (
                        <div className="space-y-12">
                            {isLoading ? (
                                <div className="space-y-8">
                                    <div>
                                        <h3 className="text-2xl font-semibold mb-4">Condominiums</h3>
                                        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                                            {[1, 2, 3].map(i => <Skeleton key={i} className="h-32 w-full rounded-xl" />)}
                                        </div>
                                    </div>
                                </div>
                            ) : isError ? (
                                <div className="text-center text-red-500 py-10">
                                    Failed to load directory search results.
                                </div>
                            ) : (
                                <>
                                    {/* Condos Section */}
                                    {data?.condos && data.condos.length > 0 && (
                                        <section>
                                            <div className="flex items-center gap-2 mb-4 text-primary">
                                                <Building2 className="h-6 w-6" />
                                                <h3 className="text-2xl font-bold">Condominiums</h3>
                                            </div>
                                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                                                {data.condos.map(condo => (
                                                    <Link key={condo.id} href={`/directory/condo/${condo.id}`}>
                                                        <Card className="h-full hover:shadow-lg transition-transform hover:-translate-y-1 cursor-pointer overflow-hidden border-border/50">
                                                            <CardHeader className="p-5 pb-3 bg-muted/30">
                                                                <CardTitle className="text-xl flex items-start justify-between">
                                                                    <span className="line-clamp-2">{condo.condo_name}</span>
                                                                    <Badge variant="secondary" className="shrink-0 ml-2">D{condo.district}</Badge>
                                                                </CardTitle>
                                                                <div className="text-sm text-muted-foreground flex items-center gap-1 mt-2">
                                                                    <MapPin className="h-4 w-4 shrink-0" />
                                                                    <span className="truncate">{condo.street_name || "Unknown street"}</span>
                                                                </div>
                                                            </CardHeader>
                                                            <CardContent className="p-5 pt-4 text-sm">
                                                                <p><span className="font-medium">Type:</span> {condo.property_type || "Condominium"}</p>
                                                                <p><span className="font-medium">Total Units:</span> {condo.total_units || "Unknown"}</p>
                                                            </CardContent>
                                                        </Card>
                                                    </Link>
                                                ))}
                                            </div>
                                        </section>
                                    )}

                                    {/* HDBs Section */}
                                    {data?.hdbs && data.hdbs.length > 0 && (
                                        <section>
                                            <div className="flex items-center gap-2 mb-4 text-primary mt-12">
                                                <Building className="h-6 w-6" />
                                                <h3 className="text-2xl font-bold">HDB Blocks</h3>
                                            </div>
                                            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                                                {data.hdbs.map(hdb => (
                                                    <Link key={hdb.hdb_id} href={`/directory/hdb/${hdb.hdb_id}`}>
                                                        <Card className="h-full hover:shadow-lg transition-transform hover:-translate-y-1 cursor-pointer border-border/50">
                                                            <CardHeader className="p-5 pb-3 bg-muted/30">
                                                                <CardTitle className="text-xl">
                                                                    Block {hdb.block_number}
                                                                </CardTitle>
                                                                <div className="text-sm text-muted-foreground flex justify-between mt-2 tracking-tight">
                                                                    <span className="truncate flex items-center gap-1">
                                                                        <MapPin className="h-4 w-4 shrink-0" />
                                                                        {hdb.street_name}
                                                                    </span>
                                                                </div>
                                                            </CardHeader>
                                                            <CardContent className="p-5 pt-4 text-sm">
                                                                <p><span className="font-medium">Town:</span> {hdb.town}</p>
                                                                <p><span className="font-medium">Total Units:</span> {hdb.total_dwelling_units || "Unknown"}</p>
                                                            </CardContent>
                                                        </Card>
                                                    </Link>
                                                ))}
                                            </div>
                                        </section>
                                    )}

                                    {(!data?.condos.length && !data?.hdbs.length) && (
                                        <div className="text-center py-20 text-muted-foreground border-2 border-dashed rounded-xl">
                                            No properties found matching "{activeQuery}".
                                        </div>
                                    )}
                                </>
                            )}
                        </div>
                    )}
                </div>
            </main>

            <footer className="border-t py-8 bg-muted/30 mt-auto text-center text-sm text-muted-foreground">
                <p>© 2026 SgEstate Directory.</p>
            </footer>
        </div>
    );
}
