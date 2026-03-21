"use client";

import { useState, useEffect } from "react";
import { Navbar } from "@/components/layout/navbar";
import { Button } from "@/components/ui/button";
import { Listing } from "@/types";
import api from "@/lib/api";
import Link from "next/link";
import { Trash2, ExternalLink, Scale } from "lucide-react";

export default function ShortlistPage() {
  const [listings, setListings] = useState<Listing[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadShortlist();
  }, []);

  const loadShortlist = async () => {
    setLoading(true);
    const savedIds: number[] = JSON.parse(localStorage.getItem("property_shortlist") || "[]");
    
    if (savedIds.length === 0) {
      setListings([]);
      setLoading(false);
      return;
    }

    try {
      const fetches = savedIds.map(id => api.get(`/listings/${id}`));
      const responses = await Promise.all(fetches);
      setListings(responses.map(r => r.data));
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  const removeListing = (id: number) => {
    const savedIds: number[] = JSON.parse(localStorage.getItem("property_shortlist") || "[]");
    const newSaved = savedIds.filter(savedId => savedId !== id);
    localStorage.setItem("property_shortlist", JSON.stringify(newSaved));
    loadShortlist();
  };

  return (
    <div className="min-h-screen bg-gray-50/50">
      <Navbar />
      <div className="container mx-auto px-4 py-12 max-w-6xl">
        <div className="flex items-center gap-3 mb-2">
            <Scale className="h-8 w-8 text-rose-500" />
            <h1 className="text-3xl font-bold">Property Shortlist & Comparison</h1>
        </div>
        <p className="text-muted-foreground mb-8 text-lg">Compare up to 3 properties side-by-side using local session memory to make your final decision.</p>

        {loading ? (
          <div className="text-center py-20 animate-pulse text-gray-400">Loading your saved properties...</div>
        ) : listings.length === 0 ? (
          <div className="text-center py-20 bg-white rounded-xl border border-dashed border-gray-300">
            <h3 className="text-xl font-medium text-gray-500 mb-4">No properties saved yet</h3>
            <Button asChild><Link href="/directory">Browse Property Directory</Link></Button>
          </div>
        ) : (
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
            {listings.map(listing => (
              <div key={listing.id} className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex flex-col">
                <div className="h-48 relative bg-gray-100">
                  {listing.image_url ? (
                      <img src={listing.image_url} alt={listing.title} className="w-full h-full object-cover" />
                  ) : (
                      <div className="w-full h-full flex items-center justify-center text-gray-400">No Image</div>
                  )}
                  <Button variant="destructive" size="icon" className="absolute top-2 right-2 h-8 w-8 rounded-full" onClick={() => removeListing(listing.id)}>
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
                
                <div className="p-6 flex-grow space-y-4">
                  <div>
                    <div className="text-xl font-bold text-gray-900 line-clamp-1" title={listing.title}>{listing.title}</div>
                    <div className="text-sm text-gray-500">{listing.district ? `District ${listing.district}` : "Singapore"}</div>
                  </div>
                  
                  <div className="text-2xl font-black text-rose-600">
                    S${listing.price?.toLocaleString() || "P.O.A"}
                  </div>
                  
                  <div className="space-y-3 pt-4 border-t border-gray-100">
                    <div className="flex justify-between items-center text-sm">
                      <span className="text-gray-500">Property Type</span>
                      <span className="font-semibold px-2 py-0.5 bg-gray-100 rounded text-gray-700">{listing.property_type || "-"}</span>
                    </div>
                    <div className="flex justify-between items-center text-sm">
                      <span className="text-gray-500">Tenure</span>
                      <span className="font-semibold text-gray-700">{listing.tenure || "-"}</span>
                    </div>
                    <div className="flex justify-between items-center text-sm">
                      <span className="text-gray-500">Floor Area</span>
                      <span className="font-semibold text-gray-700">{listing.sqft?.toLocaleString() || "-"} sqft</span>
                    </div>
                    <div className="flex items-center justify-between text-sm">
                        <span className="text-gray-500">Price Per Sqft</span>
                        <span className="font-semibold px-2 py-0.5 bg-blue-50 text-blue-700 rounded">
                            S${listing.psf ? listing.psf.toLocaleString(undefined, {maximumFractionDigits: 0}) : "-"} psf
                        </span>
                    </div>
                    <div className="flex justify-between items-center text-sm">
                      <span className="text-gray-500">Rooms</span>
                      <span className="font-semibold text-gray-700">{listing.beds} Bed, {listing.baths} Bath</span>
                    </div>
                    <div className="flex justify-between items-center text-sm">
                      <span className="text-gray-500">Built Year</span>
                      <span className="font-semibold text-gray-700">{listing.built_year ? String(listing.built_year).replace(/\D/g, "") : "-"}</span>
                    </div>
                  </div>
                </div>
                
                <div className="p-4 bg-gray-50 border-t border-gray-100 mt-auto">
                    <Button variant="outline" className="w-full bg-white text-gray-700 hover:text-rose-600" asChild>
                      <Link href={`/listings/${listing.id}`}>View Full Analysis <ExternalLink className="ml-2 h-4 w-4" /></Link>
                    </Button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
