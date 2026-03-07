"use client";

import { useState, useEffect, useCallback } from "react";
import { Heart } from "lucide-react";
import { Button } from "@/components/ui/button";
import { favouritesApi } from "@/lib/api";
import { useAuth } from "@/lib/auth-context";
import { cn } from "@/lib/utils";

interface FavouriteButtonProps {
  listingId: number;
  /** compact = icon-only; default = icon + label */
  variant?: "compact" | "default";
  className?: string;
}

export function FavouriteButton({
  listingId,
  variant = "compact",
  className,
}: FavouriteButtonProps) {
  const { user } = useAuth();
  const [isFav, setIsFav] = useState(false);
  const [loading, setLoading] = useState(false);

  // Check initial status (only when logged in)
  useEffect(() => {
    if (!user) return;
    favouritesApi
      .status(listingId)
      .then((res) => setIsFav(res.data.is_favourited))
      .catch(() => {});
  }, [listingId, user]);

  const toggle = useCallback(
    async (e: React.MouseEvent) => {
      e.preventDefault(); // stop the parent <Link> from firing
      e.stopPropagation();
      if (!user) {
        window.location.href = "/login";
        return;
      }
      setLoading(true);
      try {
        if (isFav) {
          await favouritesApi.remove(listingId);
          setIsFav(false);
        } else {
          await favouritesApi.add(listingId);
          setIsFav(true);
        }
      } catch {
        // silently ignore 409 (already fav'd)
      } finally {
        setLoading(false);
      }
    },
    [isFav, listingId, user]
  );

  return (
    <Button
      variant="ghost"
      size={variant === "compact" ? "icon" : "sm"}
      onClick={toggle}
      disabled={loading}
      aria-label={isFav ? "Remove from favourites" : "Add to favourites"}
      className={cn(
        "transition-colors",
        isFav ? "text-red-500 hover:text-red-600" : "text-muted-foreground hover:text-red-400",
        className
      )}
    >
      <Heart
        className={cn("h-4 w-4", isFav && "fill-current")}
      />
      {variant === "default" && (
        <span className="ml-1">{isFav ? "Saved" : "Save"}</span>
      )}
    </Button>
  );
}
