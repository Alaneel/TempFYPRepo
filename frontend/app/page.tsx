"use client";

import { useQuery } from "@tanstack/react-query";
import api from "@/lib/api";
import { Listing, PaginatedResponse } from "@/types";
import { ListingCard } from "@/components/features/listings/listing-card";
import { Navbar } from "@/components/layout/navbar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Search, MapPin, Sparkles } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { Skeleton } from "@/components/ui/skeleton";

export default function Home() {
  const router = useRouter();
  const [q, setQ] = useState("");

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    router.push(`/listings?q=${encodeURIComponent(q)}`);
  };

  const { data, isLoading } = useQuery<PaginatedResponse<Listing>>({
    queryKey: ["listings", "featured"],
    queryFn: async () => {
      const response = await api.get("/listings/", { params: { limit: 8 } });
      return response.data;
    },
  });

  return (
    <div className="min-h-screen flex flex-col bg-background">
      <Navbar />
      
      {/* Hero Section */}
      <section className="relative h-[500px] flex items-center justify-center bg-gray-900 text-white overflow-hidden">
        {/* Background Image Placeholder */}
        <div className="absolute inset-0 bg-gradient-to-r from-black/80 to-black/40 z-10" />
        <div 
          className="absolute inset-0 bg-cover bg-center opacity-60"
          style={{ backgroundImage: "url('/placeholders/condo_1.png')" }} 
        />
        
        <div className="relative z-20 container mx-auto px-4 text-center">
          <h1 className="text-4xl md:text-6xl font-extrabold mb-6 tracking-tight">
            Find Your Dream Home in Singapore
          </h1>
          <p className="text-xl md:text-2xl mb-8 max-w-2xl mx-auto text-gray-200">
            Browse thousands of condos, landed properties, and HDBs with real-time updates.
          </p>
          
          <form onSubmit={handleSearch} className="max-w-3xl mx-auto bg-white p-2 rounded-lg shadow-2xl flex flex-col md:flex-row gap-2">
            <div className="relative flex-grow">
               <MapPin className="absolute left-3 top-3 h-5 w-5 text-gray-400" />
               <Input
                 placeholder="Search by location, MRT, project, or describe what you want..."
                 className="pl-10 h-12 text-black border-none focus-visible:ring-0 text-base"
                 value={q}
                 onChange={(e) => setQ(e.target.value)}
               />
            </div>
            <Button size="lg" className="h-12 px-6 text-base" type="submit">
              <Search className="mr-2 h-5 w-5" />
              Search
            </Button>
            <Button
              size="lg"
              type="button"
              className="h-12 px-6 text-base bg-violet-600 hover:bg-violet-700 text-white"
              onClick={() => router.push(`/listings?mode=ai&q=${encodeURIComponent(q)}`)}
            >
              <Sparkles className="mr-2 h-5 w-5" />
              AI Search
            </Button>
          </form>
          
          <div className="mt-8 flex gap-4 justify-center">
             <Link href="/listings" className="text-sm font-medium hover:underline">Browse Condos</Link>
             <span className="text-gray-500">•</span>
             <Link href="/listings?property_type=Landed" className="text-sm font-medium hover:underline">New Launches</Link>
             <span className="text-gray-500">•</span>
             <Link href="/agents" className="text-sm font-medium hover:underline">Find an Agent</Link>
          </div>
        </div>
      </section>

      {/* Featured Listings */}
      <section className="py-16 container mx-auto px-4">
        <div className="flex justify-between items-end mb-8">
          <div>
            <h2 className="text-3xl font-bold tracking-tight mb-2">Featured Listings</h2>
            <p className="text-muted-foreground">Handpicked properties just for you.</p>
          </div>
          <Button variant="outline" asChild>
            <Link href="/listings">View All</Link>
          </Button>
        </div>
        
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
          {isLoading ? (
            Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="space-y-3">
                 <Skeleton className="h-[200px] w-full rounded-xl" />
                 <Skeleton className="h-4 w-3/4" />
                 <Skeleton className="h-4 w-1/2" />
              </div>
            ))
          ) : (
            data?.data.map((listing) => (
              <ListingCard key={listing.id} listing={listing} />
            ))
          )}
        </div>
      </section>
      
      {/* Footer (Simplified) */}
      <footer className="border-t py-12 bg-muted/30 mt-auto">
         <div className="container mx-auto px-4 text-center text-muted-foreground">
            <p>© 2026 SgEstate. Built for Singapore Real Estate.</p>
         </div>
      </footer>
    </div>
  );
}
