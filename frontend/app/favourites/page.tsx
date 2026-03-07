"use client";

import { useEffect, useState } from "react";
import { Listing } from "@/types";
import { favouritesApi } from "@/lib/api";
import { ListingCard } from "@/components/features/listings/listing-card";
import { Heart, Loader2 } from "lucide-react";
import Link from "next/link";
import { Button } from "@/components/ui/button";

export default function FavouritesPage() {
  const [listings, setListings] = useState<Listing[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    favouritesApi
      .list()
      .then((res) => setListings(res.data))
      .catch((err) => {
        if (err.response?.status === 401) {
          setError("Please log in to view your saved listings.");
        } else {
          setError("Failed to load favourites.");
        }
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-[60vh]">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <Heart className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
        <p className="text-muted-foreground mb-4">{error}</p>
        <Button asChild><Link href="/login">Log In</Link></Button>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex items-center gap-3 mb-8">
        <Heart className="h-6 w-6 text-red-500 fill-current" />
        <h1 className="text-2xl font-bold">Saved Listings</h1>
        <span className="text-muted-foreground text-sm">({listings.length})</span>
      </div>

      {listings.length === 0 ? (
        <div className="text-center py-20">
          <Heart className="h-16 w-16 mx-auto mb-4 text-muted-foreground/40" />
          <p className="text-lg text-muted-foreground mb-2">No saved listings yet</p>
          <p className="text-sm text-muted-foreground mb-6">
            Click the ❤️ on any listing to save it here.
          </p>
          <Button asChild variant="outline">
            <Link href="/listings">Browse Listings</Link>
          </Button>
        </div>
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
          {listings.map((listing) => (
            <ListingCard key={listing.id} listing={listing} />
          ))}
        </div>
      )}
    </div>
  );
}
