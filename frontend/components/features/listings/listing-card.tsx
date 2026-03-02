import Link from "next/link";
import { Listing } from "@/types";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Avatar, AvatarImage, AvatarFallback } from "@/components/ui/avatar";
import { BedDouble, Bath, Ruler, MapPin } from "lucide-react";

interface ListingCardProps {
  listing: Listing;
}

export function ListingCard({ listing }: ListingCardProps) {
  const formatPrice = (price?: number, display?: string) => {
    if (display) return display;
    if (!price) return "Price on Ask";
    return new Intl.NumberFormat('en-SG', { style: 'currency', currency: 'SGD', maximumFractionDigits: 0 }).format(price);
  };

// Deterministic image generator based on listing ID and type
  const getListingImage = (listing: Listing) => {
    // If backend provides an image, use it!
    if (listing.image_url) {
        return listing.image_url;
    }
    
    // Fallback deterministic generator
    const images = {
      Condo: [
        "https://images.unsplash.com/photo-1486406146926-c627a92ad1ab?auto=format&fit=crop&w=800&q=80", // High rise
        "https://images.unsplash.com/photo-1545324418-cc1a3fa10c00?auto=format&fit=crop&w=800&q=80", // Modern apt
        "https://images.unsplash.com/photo-1515263487990-61b07816b324?auto=format&fit=crop&w=800&q=80", // Condo balcony
        "https://images.unsplash.com/photo-1512917774080-9991f1c4c750?auto=format&fit=crop&w=800&q=80", // Luxury home
      ],
      HDB: [
        "https://images.unsplash.com/photo-1626284620022-262254de5c01?auto=format&fit=crop&w=800&q=80", // HDB blocks
        "https://images.unsplash.com/photo-1634552402120-dcee914e6b5d?auto=format&fit=crop&w=800&q=80", // HDB exterior
        "https://images.unsplash.com/photo-1599557452655-226e47c134bf?auto=format&fit=crop&w=800&q=80", // HDB corridor
      ],
      Landed: [
        "https://images.unsplash.com/photo-1600596542815-2250657d2fc5?auto=format&fit=crop&w=800&q=80", // Modern house
        "https://images.unsplash.com/photo-1564013799919-ab600027ffc6?auto=format&fit=crop&w=800&q=80", // Villa
        "https://images.unsplash.com/photo-1600607687939-ce8a6c25118c?auto=format&fit=crop&w=800&q=80", // Luxury house
      ]
    };

    let category = "Condo";
    const type = listing.property_type?.toLowerCase() || "";
    if (type.includes("hdb")) category = "HDB";
    else if (type.includes("landed") || type.includes("bungalow") || type.includes("terrace")) category = "Landed";
    
    // Deterministic selection using ID
    const list = images[category as keyof typeof images];
    const index = (listing.id || 0) % list.length;
    return list[index];
  };

  const imageUrl = getListingImage(listing);

  return (
    <Link href={`/listings/${listing.id}`} className="block h-full">
      <Card className="h-full overflow-hidden hover:shadow-lg transition-shadow duration-300 flex flex-col group cursor-pointer">
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
            {listing.buy_rent === 'Rent' ? 'For Rent' : 'For Sale'}
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
             <span className="truncate">{listing.address || listing.street_name || "Singapore"}</span>
             {listing.district && <span className="ml-1 text-xs px-1.5 py-0.5 bg-muted rounded">D{listing.district}</span>}
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
                <span className="truncate">{listing.sqft ? `${listing.sqft.toLocaleString()} sqft` : "-"}</span>
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
               <span className="truncate font-medium text-foreground">
                 {listing.agent.name || "Contact Agent"}
               </span>
             </div>
           )}
           <div className="flex justify-between w-full items-center gap-2">
              <span className="truncate">{listing.tenure || "Unknown Tenure"}</span>
              <span className="shrink-0">
                {listing.built_year && listing.built_year.replace(/\D/g, '') 
                  ? listing.built_year.replace(/\D/g, '') 
                  : "-"}
              </span>
           </div>
        </CardFooter>
      </Card>
    </Link>
  );
}
