"use client";

import { useEffect, useState } from "react";
import { RecommendationItem } from "@/types";
import { favouritesApi } from "@/lib/api";
import { ListingCard } from "@/components/features/listings/listing-card";
import { Sparkles, Loader2 } from "lucide-react";
import Link from "next/link";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";

export default function RecommendationsPage() {
  const [items, setItems] = useState<RecommendationItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    favouritesApi
      .forYou(24)
      .then((res) => setItems(res.data))
      .catch((err) => {
        if (err.response?.status === 401) {
          setError("Please log in to get personalised recommendations.");
        } else if (err.response?.status === 404) {
          setError("no_favourites");
        } else {
          setError("Failed to load recommendations.");
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

  if (error === "no_favourites") {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <Sparkles className="h-12 w-12 mx-auto mb-4 text-muted-foreground" />
        <p className="text-lg font-medium mb-2">No recommendations yet</p>
        <p className="text-sm text-muted-foreground mb-6">
          Save at least one listing to unlock personalised recommendations.
        </p>
        <Button asChild>
          <Link href="/listings">Browse &amp; Save Listings</Link>
        </Button>
      </div>
    );
  }

  if (error) {
    return (
      <div className="container mx-auto px-4 py-16 text-center">
        <p className="text-muted-foreground mb-4">{error}</p>
        <Button asChild>
          <Link href="/login">Log In</Link>
        </Button>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="flex items-center gap-3 mb-2">
        <Sparkles className="h-6 w-6 text-primary" />
        <h1 className="text-2xl font-bold">For You</h1>
      </div>
      <p className="text-sm text-muted-foreground mb-8">
        Personalised picks based on your saved listings — ranked by property type,
        district, price range, and bedroom count.
      </p>

      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
        {items.map(({ listing, score, match_reasons }) => (
          <div key={listing.id} className="relative">
            <ListingCard listing={listing} />
            <div className="absolute top-2 left-2 z-10">
              <Badge
                className="bg-primary/90 text-primary-foreground text-xs cursor-default"
                title={match_reasons.join(" · ")}
              >
                {Math.round(score * 100)}% match
              </Badge>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
