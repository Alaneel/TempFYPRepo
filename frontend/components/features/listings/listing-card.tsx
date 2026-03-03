import Link from "next/link";
import { useEffect, useRef } from "react";
import { Listing } from "@/types";
import {
  Card,
  CardContent,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Avatar, AvatarImage, AvatarFallback } from "@/components/ui/avatar";
import { BedDouble, Bath, Ruler, MapPin, Star } from "lucide-react";

interface ListingCardProps {
  listing: Listing;
  isHighlighted?: boolean;
}

export function ListingCard({ listing, isHighlighted = false }: ListingCardProps) {
  const cardRef = useRef<HTMLDivElement>(null);

  // 高亮时自动滚动到可见位置
  useEffect(() => {
    if (isHighlighted && cardRef.current) {
      cardRef.current.scrollIntoView({ behavior: "smooth", block: "nearest" });
    }
  }, [isHighlighted]);
  const formatPrice = (price?: number, display?: string) => {
    if (display) return display;
    if (!price) return "Price on Ask";
    return new Intl.NumberFormat("en-SG", {
      style: "currency",
      currency: "SGD",
      maximumFractionDigits: 0,
    }).format(price);
  };

  // Deterministic image generator based on listing ID and type
  const getListingImage = (listing: Listing) => {
    // If backend provides an image, use it!
    if (listing.image_url) {
      return listing.image_url;
    }

    // Fallback deterministic generator — 用本地图避免外链挂掉
    const images = {
      Condo: ["/placeholders/condo_0.png", "/placeholders/condo_1.png", "/placeholders/condo.png"],
      HDB:   ["/placeholders/hdb_0.png", "/placeholders/hdb_1.png", "/placeholders/hdb_2.png", "/placeholders/hdb_3.png"],
      Landed:["/placeholders/landed_0.png", "/placeholders/landed_1.png", "/placeholders/landed_2.png", "/placeholders/landed_3.png"],
    };

    let category = "Condo";
    const type = listing.property_type?.toLowerCase() || "";
    if (type.includes("hdb")) category = "HDB";
    else if (
      type.includes("landed") ||
      type.includes("bungalow") ||
      type.includes("terrace")
    )
      category = "Landed";

    // Deterministic selection using ID
    const list = images[category as keyof typeof images];
    const index = (listing.id || 0) % list.length;
    return list[index];
  };

  const imageUrl = getListingImage(listing);

  return (
    <div ref={cardRef}>
    <Link href={`/listings/${listing.id}`} className="block h-full">
      <Card className={`h-full overflow-hidden hover:shadow-lg transition-all duration-300 flex flex-col group cursor-pointer ${isHighlighted ? "ring-2 ring-slate-900 shadow-xl scale-[1.02]" : ""}`}>
        <div className="relative aspect-video bg-gray-200 overflow-hidden">
          <img
            src={imageUrl}
            alt={listing.title}
            className="w-full h-full object-cover group-hover:scale-110 transition-transform duration-500"
          />
          <Badge className="absolute top-2 left-2 bg-black/70 hover:bg-black/80">
            {listing.property_type || "Condo"}
          </Badge>
          <Badge variant="secondary" className="absolute top-2 right-2">
            {listing.buy_rent === "Rent" ? "For Rent" : "For Sale"}
          </Badge>
        </div>

        <CardHeader className="p-4 pb-2">
          <div className="flex justify-between items-start">
            <CardTitle className="text-lg font-semibold line-clamp-2 min-h-[3rem]">
              {listing.title}
            </CardTitle>
          </div>
          <div className="flex items-center text-muted-foreground text-sm mt-1">
            <MapPin className="h-3.5 w-3.5 mr-1" />
            <span className="truncate">
              {listing.address || listing.street_name || "Singapore"}
            </span>
            {listing.district && (
              <span className="ml-1 text-xs px-1.5 py-0.5 bg-muted rounded">
                D{listing.district}
              </span>
            )}
          </div>
        </CardHeader>

        <CardContent className="p-4 pt-2 flex-grow">
          <div className="text-xl font-bold text-primary mb-3">
            {formatPrice(listing.price, listing.display_price)}
          </div>

          <div className="grid grid-cols-3 gap-2 text-sm text-muted-foreground">
            <div className="flex items-center gap-1">
              <BedDouble className="h-4 w-4" />
              <span>{listing.beds || 0} Beds</span>
            </div>
            <div className="flex items-center gap-1">
              <Bath className="h-4 w-4" />
              <span>{listing.baths || 0} Baths</span>
            </div>
            <div className="flex items-center gap-1 overflow-hidden">
              <Ruler className="h-4 w-4 shrink-0" />
              <span className="truncate">
                {listing.sqft ? `${listing.sqft.toLocaleString()} sqft` : "-"}
              </span>
            </div>
          </div>
        </CardContent>

        <CardFooter className="p-4 pt-3 flex-col gap-3 text-xs text-muted-foreground border-t bg-muted/10">
          {listing.agent && (
            <div className="flex items-center gap-2 w-full">
              <Avatar className="h-6 w-6 border">
                <AvatarImage
                  src={
                    listing.agent.photo_url
                      ? listing.agent.photo_url.startsWith("http")
                        ? listing.agent.photo_url
                        : `${process.env.NEXT_PUBLIC_API_URL?.replace("/api/v1", "") || "http://localhost:8000"}${listing.agent.photo_url}`
                      : undefined
                  }
                  alt={listing.agent.name || "Agent"}
                />
                <AvatarFallback className="text-[10px] bg-primary/10 text-primary">
                  {(listing.agent.name || "A").charAt(0).toUpperCase()}
                </AvatarFallback>
              </Avatar>
              <span className="truncate font-medium text-foreground flex-1">
                {listing.agent.name || "Contact Agent"}
              </span>
              {listing.agent.rating != null && (
                <span className="flex items-center gap-0.5 text-yellow-500 shrink-0">
                  <Star className="h-3 w-3 fill-current" />
                  <span className="text-[11px] font-medium">
                    {listing.agent.rating.toFixed(1)}
                  </span>
                </span>
              )}
            </div>
          )}
          <div className="flex justify-between w-full items-center gap-2">
            <span className="truncate">
              {listing.tenure || "Unknown Tenure"}
            </span>
            <span className="shrink-0">
              {listing.built_year && listing.built_year.replace(/\D/g, "")
                ? listing.built_year.replace(/\D/g, "")
                : "-"}
            </span>
          </div>
        </CardFooter>
      </Card>
    </Link>
    </div>
  );
}
