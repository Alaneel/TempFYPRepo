"use client";

import { useQuery } from "@tanstack/react-query";
import { useParams } from "next/navigation";
import Link from "next/link";
import api from "@/lib/api";
import { Listing } from "@/types";
import { Navbar } from "@/components/layout/navbar";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { Card, CardContent } from "@/components/ui/card";
import {
  MapPin,
  BedDouble,
  Bath,
  Ruler,
  Building,
  Calendar,
  User,
  Phone,
  FileText,
  Hash,
  Home,
  DollarSign,
  ExternalLink,
} from "lucide-react";
import { ValuationPanel } from "@/components/features/listings/valuation-panel";
import MapView from "@/components/features/map/map-view";

export default function ListingDetailPage() {
  const { id } = useParams();

  const {
    data: listing,
    isLoading,
    isError,
  } = useQuery<Listing>({
    queryKey: ["listing", id],
    queryFn: async () => {
      const response = await api.get(`/listings/${id}`);
      return response.data;
    },
    enabled: !!id,
  });

  const formatPrice = (price?: number, display?: string) => {
    if (display) return display;
    if (!price) return "Price on Ask";
    return new Intl.NumberFormat("en-SG", {
      style: "currency",
      currency: "SGD",
      maximumFractionDigits: 0,
    }).format(price);
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background">
        <Navbar />
        <div className="container mx-auto px-4 py-8 space-y-8">
          <Skeleton className="h-[400px] w-full rounded-xl" />
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
            <div className="lg:col-span-2 space-y-4">
              <Skeleton className="h-10 w-3/4" />
              <Skeleton className="h-4 w-1/2" />
              <Skeleton className="h-60 w-full" />
            </div>
            <div className="space-y-4">
              <Skeleton className="h-60 w-full" />
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (isError || !listing) {
    return (
      <div className="min-h-screen bg-background text-center py-20">
        <Navbar />
        <h1 className="text-2xl font-bold mt-10">Listing not found</h1>
        <Button className="mt-4" asChild>
          <Link href="/listings">Back to Listings</Link>
        </Button>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-background">
      <Navbar />

      {/* Hero / Images */}
      <div className="bg-gray-100 h-[400px] md:h-[500px] relative">
        <div className="absolute inset-0 flex items-center justify-center text-gray-400">
          <span className="text-xl font-medium">Image Gallery Placeholder</span>
        </div>
        <div className="absolute bottom-4 right-4 flex gap-2">
          <Button variant="secondary">View Photos (5)</Button>
          <Button variant="secondary">View Map</Button>
        </div>
      </div>

      <div className="container mx-auto px-4 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Left Column: Main Info */}
          <div className="lg:col-span-2 space-y-8">
            <div>
              <div className="flex flex-wrap gap-2 mb-3">
                <Badge>{listing.property_type}</Badge>
                <Badge variant="outline">
                  {listing.buy_rent === "Rent" ? "For Rent" : "For Sale"}
                </Badge>
                {listing.tenure && (
                  <Badge variant="secondary">{listing.tenure}</Badge>
                )}
              </div>
              <h1 className="text-3xl md:text-4xl font-bold mb-2">
                {listing.title}
              </h1>
              <div className="flex items-center text-muted-foreground text-lg">
                <MapPin className="h-5 w-5 mr-2" />
                {listing.address || listing.street_name || "Singapore"}{" "}
                {listing.district && `(D${listing.district})`}
              </div>
            </div>

            <div className="grid grid-cols-3 md:grid-cols-4 gap-4 py-6 border-y">
              <div className="flex flex-col">
                <span className="text-muted-foreground text-sm flex items-center gap-1">
                  <BedDouble className="h-4 w-4" /> Beds
                </span>
                <span className="text-xl font-semibold">
                  {listing.beds || 0}
                </span>
              </div>
              <div className="flex flex-col">
                <span className="text-muted-foreground text-sm flex items-center gap-1">
                  <Bath className="h-4 w-4" /> Baths
                </span>
                <span className="text-xl font-semibold">
                  {listing.baths || 0}
                </span>
              </div>
              <div className="flex flex-col">
                <span className="text-muted-foreground text-sm flex items-center gap-1">
                  <Ruler className="h-4 w-4" /> Sqft
                </span>
                <span className="text-xl font-semibold">
                  {listing.sqft ? listing.sqft.toLocaleString() : "-"}
                </span>
              </div>
              <div className="flex flex-col">
                <span className="text-muted-foreground text-sm flex items-center gap-1">
                  <Building className="h-4 w-4" /> Built
                </span>
                <span className="text-xl font-semibold">
                  {listing.built_year
                    ? listing.built_year.replace(/\D/g, "")
                    : "N/A"}
                </span>
              </div>
            </div>

            <div className="space-y-4">
              <h2 className="text-xl font-bold">Description</h2>
              <div className="prose max-w-none text-muted-foreground whitespace-pre-line">
                {listing.description || "No description provided."}
              </div>
            </div>

            {listing.facilities_json && (
              <div className="space-y-4">
                <h2 className="text-xl font-bold">Facilities</h2>
                <div className="flex flex-wrap gap-2">
                  {(() => {
                    try {
                      const facilities = JSON.parse(listing.facilities_json);
                      if (Array.isArray(facilities)) {
                        return facilities.map((facility, index) => (
                          <Badge
                            key={index}
                            variant="secondary"
                            className="px-3 py-1 text-sm font-normal"
                          >
                            {facility}
                          </Badge>
                        ));
                      }
                      return (
                        <p className="text-muted-foreground">
                          {listing.facilities_json}
                        </p>
                      );
                    } catch (e) {
                      return (
                        <p className="text-muted-foreground">
                          {listing.facilities_json}
                        </p>
                      );
                    }
                  })()}
                </div>
              </div>
            )}

            {/* Property Details Section */}
            <div className="space-y-4">
              <h2 className="text-xl font-bold">Property Details</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-y-4 gap-x-8">
                {(listing.condo?.top_date || listing.built_year) && (
                  <div className="flex items-start gap-3">
                    <FileText className="h-5 w-5 text-muted-foreground mt-0.5" />
                    <div>
                      <div className="text-sm font-medium">
                        {listing.condo?.top_date || listing.built_year}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        TOP Year
                      </div>
                    </div>
                  </div>
                )}

                {(listing.tenure || listing.condo?.tenure) && (
                  <div className="flex items-start gap-3">
                    <Calendar className="h-5 w-5 text-muted-foreground mt-0.5" />
                    <div>
                      <div className="text-sm font-medium">
                        {listing.tenure || listing.condo?.tenure}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Tenure
                      </div>
                    </div>
                  </div>
                )}

                <div className="flex items-start gap-3">
                  <Calendar className="h-5 w-5 text-muted-foreground mt-0.5" />
                  <div>
                    <div className="text-sm font-medium">
                      {new Date(listing.created_at).toLocaleDateString(
                        "en-GB",
                        { day: "numeric", month: "short", year: "numeric" },
                      )}
                    </div>
                    <div className="text-xs text-muted-foreground">
                      Listed On
                    </div>
                  </div>
                </div>

                <div className="flex items-start gap-3">
                  <Hash className="h-5 w-5 text-muted-foreground mt-0.5" />
                  <div>
                    <div className="text-sm font-medium">{listing.id}</div>
                    <div className="text-xs text-muted-foreground">
                      Listing ID
                    </div>
                  </div>
                </div>

                {listing.sqft && (
                  <div className="flex items-start gap-3">
                    <Ruler className="h-5 w-5 text-muted-foreground mt-0.5" />
                    <div>
                      <div className="text-sm font-medium">
                        {listing.sqft.toLocaleString()} sqft
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Floor Area
                      </div>
                    </div>
                  </div>
                )}

                {listing.psf && (
                  <div className="flex items-start gap-3">
                    <DollarSign className="h-5 w-5 text-muted-foreground mt-0.5" />
                    <div>
                      <div className="text-sm font-medium">
                        S${" "}
                        {listing.psf.toLocaleString("en-SG", {
                          minimumFractionDigits: 2,
                          maximumFractionDigits: 2,
                        })}{" "}
                        psf
                      </div>
                      <div className="text-xs text-muted-foreground">PSF</div>
                    </div>
                  </div>
                )}

                {(listing.condo?.developer_name || listing.developer_name) && (
                  <div className="flex items-start gap-3">
                    <Building className="h-5 w-5 text-muted-foreground mt-0.5" />
                    <div>
                      <div className="text-sm font-medium">
                        {listing.condo?.developer_name ||
                          listing.developer_name}
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Developer
                      </div>
                    </div>
                  </div>
                )}

                {listing.condo?.total_units && (
                  <div className="flex items-start gap-3">
                    <Home className="h-5 w-5 text-muted-foreground mt-0.5" />
                    <div>
                      <div className="text-sm font-medium">
                        {listing.condo.total_units} units
                      </div>
                      <div className="text-xs text-muted-foreground">
                        Total Units
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div className="space-y-4">
              <h2 className="text-xl font-bold">Location</h2>
              <div className="h-[400px] w-full rounded-xl overflow-hidden border">
                <MapView
                  listings={listing ? [listing] : []}
                  singleListing={true}
                  zoom={14}
                />
              </div>
            </div>
          </div>

          {/* Right Column: Price & Agent */}
          <div className="space-y-6">
            <Card className="sticky top-24">
              <CardContent className="p-6 space-y-6">
                <div>
                  <div className="text-sm text-muted-foreground">Price</div>
                  <div className="text-3xl font-bold text-primary">
                    {formatPrice(listing.price, listing.display_price)}
                  </div>
                  {listing.psf && (
                    <div className="text-sm text-muted-foreground mt-1">
                      S$
                      {listing.psf.toLocaleString("en-SG", {
                        minimumFractionDigits: 2,
                        maximumFractionDigits: 2,
                      })}{" "}
                      psf
                    </div>
                  )}
                </div>

                <Separator />

                <div className="space-y-4">
                  <div className="flex items-center gap-3">
                    <div className="h-12 w-12 rounded-full bg-primary/10 flex items-center justify-center overflow-hidden cursor-pointer hover:opacity-80 transition-opacity">
                      <Link href={`/agents/${listing.agent?.id}`}>
                        {listing.agent?.photo_url ? (
                          <img
                            src={
                              listing.agent.photo_url.startsWith("http")
                                ? listing.agent.photo_url
                                : `${process.env.NEXT_PUBLIC_API_URL?.replace("/api/v1", "") || "http://localhost:8000"}${listing.agent.photo_url}`
                            }
                            alt={listing.agent.name}
                            className="w-full h-full object-cover"
                          />
                        ) : (
                          <User className="h-6 w-6 text-primary" />
                        )}
                      </Link>
                    </div>
                    <div className="flex-grow">
                      <Link
                        href={`/agents/${listing.agent?.id}`}
                        className="hover:underline"
                      >
                        <div className="font-semibold">
                          {listing.agent?.name || "Unknown Agent"}
                        </div>
                      </Link>
                      <div className="text-xs text-muted-foreground">
                        {listing.agent?.cea
                          ? `CEA: ${listing.agent.cea}`
                          : "Registered Agent"}
                      </div>
                      {listing.agent?.company_name && (
                        <div className="text-xs text-muted-foreground flex items-center gap-1 mt-0.5">
                          <Building className="h-3 w-3" />
                          {listing.agent.company_name}
                        </div>
                      )}
                    </div>
                  </div>

                  <Button className="w-full" size="lg" asChild>
                    <a href={`tel:${listing.agent?.mobile || ""}`}>
                      <Phone className="mr-2 h-4 w-4" />
                      {listing.agent?.mobile
                        ? "Contact Agent"
                        : "No Contact Info"}
                    </a>
                  </Button>
                  <Button
                    variant="outline"
                    className="w-full"
                    onClick={() => {
                      if (listing.agent?.mobile) {
                        window.open(
                          `https://wa.me/65${listing.agent.mobile.replace(/\D/g, "")}`,
                          "_blank",
                        );
                      } else {
                        alert("No mobile number available for WhatsApp");
                      }
                    }}
                  >
                    WhatsApp
                  </Button>

                  {listing.url && (
                    <Button variant="secondary" className="w-full" asChild>
                      <a
                        href={listing.url}
                        target="_blank"
                        rel="noopener noreferrer"
                      >
                        <ExternalLink className="mr-2 h-4 w-4" />
                        View Original Listing
                      </a>
                    </Button>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* AI Valuation Panel — only for whole units (beds ≥ 1) */}
            {listing.sqft &&
              listing.sqft > 50 &&
              listing.beds != null &&
              listing.beds >= 1 && (
                <ValuationPanel
                  propertyType={listing.property_type || ""}
                  buyRent={listing.buy_rent || "property-for-sale"}
                  beds={listing.beds}
                  sqft={listing.sqft}
                  tenure={listing.tenure || undefined}
                  actualPrice={listing.price || undefined}
                  builtYear={
                    listing.built_year
                      ? parseInt(
                          String(listing.built_year)
                            .replace(/\D/g, "")
                            .slice(0, 4),
                        )
                      : undefined
                  }
                />
              )}
          </div>
        </div>
      </div>
    </div>
  );
}
