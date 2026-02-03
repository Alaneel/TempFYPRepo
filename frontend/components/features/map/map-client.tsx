"use client";

import { useState, useEffect } from "react";
import { MapContainer, TileLayer, Marker, Popup, useMap, useMapEvents } from "react-leaflet";
import L from "leaflet";
import { Listing } from "@/types";
import Link from "next/link";
import { Button } from "@/components/ui/button";

// function to format price
const formatPrice = (price?: number, display?: string) => {
  if (display) return display;
  if (!price) return "?";
  if (price >= 1000000) {
      return `$${(price / 1000000).toFixed(2)}M`;
  }
  if (price >= 1000) {
      return `$${(price / 1000).toFixed(0)}K`;
  }
  return `$${price}`;
};

const createPriceIcon = (price?: number, display?: string) => {
  const priceText = formatPrice(price, display);
  return L.divIcon({
    className: 'bg-transparent border-none', 
    html: `
      <div class="relative group">
        <div class="bg-white text-slate-900 font-bold px-3 py-1.5 rounded-full shadow-md border border-slate-200 text-xs whitespace-nowrap min-w-max transition-transform transform group-hover:scale-110 group-hover:z-50 flex items-center justify-center gap-1">
           ${priceText}
        </div>
        <div class="border-solid border-t-white border-x-transparent border-t-4 border-x-4 absolute -bottom-1 left-1/2 -translate-x-1/2"></div>
      </div>
    `,
    iconSize: [60, 30],
    iconAnchor: [30, 34],
    popupAnchor: [0, -34],
  });
};

const createDotIcon = () => {
    return L.divIcon({
        className: 'bg-transparent border-none',
        html: `
            <div class="w-2.5 h-2.5 bg-white rounded-full border border-slate-300 shadow-sm hover:scale-150 transition-transform"></div>
        `,
        iconSize: [10, 10],
        iconAnchor: [5, 5],
        popupAnchor: [0, -5],
    });
};

const createHomeIcon = (price?: number, display?: string) => {
    const priceText = formatPrice(price, display);
    return L.divIcon({
        className: 'bg-transparent border-none',
        html: `
          <div class="relative z-[100]">
            <div class="bg-slate-900 text-white font-bold px-4 py-2 rounded-full shadow-xl border-2 border-white text-sm whitespace-nowrap min-w-max flex items-center justify-center gap-2">
               <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-home"><path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
               ${priceText}
            </div>
            <div class="border-solid border-t-slate-900 border-x-transparent border-t-[6px] border-x-[6px] absolute -bottom-1.5 left-1/2 -translate-x-1/2"></div>
          </div>
        `,
        iconSize: [80, 40],
        iconAnchor: [40, 44],
        popupAnchor: [0, -44],
    });
};



// Component to track zoom events
function MapEvents({ onZoomEnd }: { onZoomEnd: (zoom: number) => void }) {
    const map = useMapEvents({
        zoomend: () => {
            onZoomEnd(map.getZoom());
        },
    });
    return null;
}

interface MapClientProps {
  listings?: Listing[];
  center?: [number, number];
  zoom?: number;
  className?: string;
  singleListing?: boolean;
}

// Component to update map center when props change
function MapController({ center, zoom }: { center: [number, number]; zoom: number }) {
  const map = useMap();
  useEffect(() => {
    map.setView(center, zoom);
  }, [center[0], center[1], zoom, map]);
  return null;
}

export default function MapClient({ 
  listings = [], 
  center = [1.3521, 103.8198], // Default Singapore center
  zoom = 12,
  className = "h-full w-full rounded-lg",
  singleListing = false
}: MapClientProps) {
  
  // Calculate effective center
  let mapCenter: [number, number] = center;
  
  // If single listing, enforce center on that listing's (mock) location
  if (singleListing && listings.length > 0) {
      const listing = listings[0];
      const seed = listing.id;
      const lat = 1.3521 + (Math.sin(seed) * 0.05); 
      const lng = 103.8198 + (Math.cos(seed) * 0.05);
      mapCenter = [lat, lng];
  }

  const [currentZoom, setCurrentZoom] = useState(zoom);

  return (
    <MapContainer 
      center={mapCenter} 
      zoom={zoom} 
      className={className}
      scrollWheelZoom={true}
    >
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
        url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
      />
      <MapController center={mapCenter} zoom={zoom} />
      <MapEvents onZoomEnd={setCurrentZoom} />
      
      {listings.map((listing) => {
          // Hardcoded lat/lng logic for demo if not present (Real apps need geocoding)
          const seed = listing.id;
          const lat = 1.3521 + (Math.sin(seed) * 0.05); 
          const lng = 103.8198 + (Math.cos(seed) * 0.05);
          
          // Determine Icon based on context
          let icon;
          if (singleListing) {
              icon = createHomeIcon(listing.price, listing.display_price);
          } else {
              // Zoom threshold for Dots vs Price
              icon = currentZoom < 11
                ? createDotIcon() 
                : createPriceIcon(listing.price, listing.display_price);
          }

          return (
            <Marker 
                key={listing.id} 
                position={[lat, lng]} 
                icon={icon}
                zIndexOffset={singleListing ? 1000 : 0} // Ensure subject property is always on top
            >
              <Popup>
                <div className="min-w-[200px]">
                    <h3 className="font-semibold text-sm">{listing.title}</h3>
                    <p className="text-xs text-muted-foreground truncate">{listing.address}</p>
                    <div className="mt-2 font-bold text-primary">
                        {listing.display_price || (listing.price ? `$${listing.price.toLocaleString()}` : "Price/Ask")}
                    </div>
                    {!singleListing && (
                        <Button asChild size="sm" className="w-full mt-2" variant="outline">
                            <Link href={`/listings/${listing.id}`}>View Details</Link>
                        </Button>
                    )}
                </div>
              </Popup>
            </Marker>
          );
      })}
    </MapContainer>
  );
}
