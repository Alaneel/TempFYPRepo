"use client";

import { useState, useEffect, useMemo } from "react";
import { MapContainer, TileLayer, Marker, Popup, useMap, useMapEvents } from "react-leaflet";
import L from "leaflet";
import Supercluster from "supercluster";
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

// Airbnb-style price tag (white pill)
const createPriceIcon = (price?: number, display?: string, active = false) => {
  const priceText = formatPrice(price, display);
  const bg = active ? "bg-slate-900 text-white" : "bg-white text-slate-900";
  return L.divIcon({
    className: 'bg-transparent border-none',
    html: `
      <div class="relative">
        <div class="${bg} font-bold px-3 py-1.5 rounded-full shadow-md border border-slate-200 text-xs whitespace-nowrap min-w-max flex items-center justify-center cursor-pointer hover:scale-110 transition-transform" style="font-size:12px;font-weight:700">
           ${priceText}
        </div>
        <div class="absolute -bottom-1 left-1/2 -translate-x-1/2 w-0 h-0" style="border-left:4px solid transparent;border-right:4px solid transparent;border-top:5px solid ${active ? '#0f172a' : 'white'}"></div>
      </div>
    `,
    iconSize: [70, 32],
    iconAnchor: [35, 37],
    popupAnchor: [0, -37],
  });
};

// Airbnb-style cluster bubble (dark circle with count)
const createClusterIcon = (count: number) => {
  const size = count < 10 ? 36 : count < 100 ? 44 : 52;
  const fontSize = count < 10 ? 13 : count < 100 ? 12 : 11;
  return L.divIcon({
    className: 'bg-transparent border-none',
    html: `
      <div style="
        width:${size}px;height:${size}px;
        background:#0f172a;
        color:white;
        border-radius:50%;
        border:2.5px solid white;
        box-shadow:0 2px 8px rgba(0,0,0,0.35);
        display:flex;align-items:center;justify-content:center;
        font-weight:700;font-size:${fontSize}px;
        cursor:pointer;
      ">${count}</div>
    `,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
};

// single-listing 主页图标
const createHomeIcon = (price?: number, display?: string) => {
    const priceText = formatPrice(price, display);
    return L.divIcon({
        className: 'bg-transparent border-none',
        html: `
          <div class="relative z-[100]">
            <div class="bg-slate-900 text-white font-bold px-4 py-2 rounded-full shadow-xl border-2 border-white text-sm whitespace-nowrap min-w-max flex items-center justify-center gap-2">
               <svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><polyline points="9 22 9 12 15 12 15 22"/></svg>
               ${priceText}
            </div>
            <div class="absolute -bottom-1.5 left-1/2 -translate-x-1/2 w-0 h-0" style="border-left:6px solid transparent;border-right:6px solid transparent;border-top:7px solid #0f172a"></div>
          </div>
        `,
        iconSize: [80, 40],
        iconAnchor: [40, 47],
        popupAnchor: [0, -47],
    });
};



// Component to track zoom AND bounds events
function MapEvents({ onViewChange }: { onViewChange: (zoom: number, bounds: L.LatLngBounds) => void }) {
    const map = useMapEvents({
        zoomend: () => onViewChange(map.getZoom(), map.getBounds()),
        moveend: () => onViewChange(map.getZoom(), map.getBounds()),
    });
    return null;
}

interface MapClientProps {
  listings?: Listing[];
  center?: [number, number];
  zoom?: number;
  className?: string;
  singleListing?: boolean;
  onBoundsChange?: (bounds: { minLat: number; maxLat: number; minLng: number; maxLng: number }) => void;
}

// Component to update map center when props change
function MapController({ center, zoom }: { center: [number, number]; zoom: number }) {
  const map = useMap();
  const lat = center[0];
  const lng = center[1];
  useEffect(() => {
    map.setView([lat, lng], zoom);
  }, [lat, lng, zoom, map]);
  return null;
}

// Inner component with access to map instance for initial bounds
function ClusterLayer({ listings, singleListing, activeId, onActiveChange, onBoundsChange }: {
  listings: Listing[];
  singleListing: boolean;
  activeId: number | null;
  onActiveChange: (id: number | null) => void;
  onBoundsChange?: (bounds: { minLat: number; maxLat: number; minLng: number; maxLng: number }) => void;
}) {
  const map = useMap();
  const [currentZoom, setCurrentZoom] = useState(map.getZoom());
  const [bounds, setBounds] = useState(map.getBounds());

  const emitBounds = (b: L.LatLngBounds) => {
    onBoundsChange?.({
      minLat: b.getSouth(),
      maxLat: b.getNorth(),
      minLng: b.getWest(),
      maxLng: b.getEast(),
    });
  };

  // 初始化时触发一次，让列表知道初始视野
  useEffect(() => {
    emitBounds(map.getBounds());
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // Build supercluster index from all listings
  const supercluster = useMemo(() => {
    const sc = new Supercluster<{ listingId: number; price?: number; display_price?: string }>({
      radius: 60,
      maxZoom: 17,
      minZoom: 1,
    });
    const points: Supercluster.PointFeature<{ listingId: number; price?: number; display_price?: string }>[] = listings.map((l) => {
      const seed = l.id;
      const lat = l.latitude ?? (1.3521 + (Math.sin(seed) * 0.05));
      const lng = l.longitude ?? (103.8198 + (Math.cos(seed) * 0.05));
      return {
        type: "Feature",
        geometry: { type: "Point", coordinates: [lng, lat] },
        properties: { listingId: l.id, price: l.price, display_price: l.display_price },
      };
    });
    sc.load(points);
    return sc;
  }, [listings]);

  const handleViewChange = (zoom: number, b: L.LatLngBounds) => {
    setCurrentZoom(zoom);
    setBounds(b);
    emitBounds(b);
  };

  // Get clusters for current viewport
  const clusters = useMemo(() => {
    const bbox: [number, number, number, number] = [
      bounds.getWest(),
      bounds.getSouth(),
      bounds.getEast(),
      bounds.getNorth(),
    ];
    return supercluster.getClusters(bbox, Math.round(currentZoom));
  }, [supercluster, bounds, currentZoom]);

  return (
    <>
      <MapEvents onViewChange={handleViewChange} />
      {clusters.map((cluster, idx) => {
        const [lng, lat] = cluster.geometry.coordinates;
        const { cluster: isCluster, point_count } = cluster.properties as { cluster?: boolean; point_count?: number; listingId?: number; price?: number; display_price?: string };

        if (isCluster && point_count) {
          // --- Cluster bubble ---
          return (
            <Marker
              key={`cluster-${idx}`}
              position={[lat, lng]}
              icon={createClusterIcon(point_count)}
              eventHandlers={{
                click: () => {
                  const expansionZoom = Math.min(
                    supercluster.getClusterExpansionZoom((cluster.properties as { cluster_id: number }).cluster_id),
                    17
                  );
                  map.flyTo([lat, lng], expansionZoom, { duration: 0.5 });
                },
              }}
            />
          );
        }

        // --- Individual price tag ---
        const { listingId, price, display_price } = cluster.properties as { listingId: number; price?: number; display_price?: string };
        const listing = listings.find((l) => l.id === listingId);
        const isActive = activeId === listingId;

        return (
          <Marker
            key={`listing-${listingId}`}
            position={[lat, lng]}
            icon={createPriceIcon(price, display_price, isActive)}
            zIndexOffset={isActive ? 500 : 0}
            eventHandlers={{
              click: () => onActiveChange(isActive ? null : listingId),
            }}
          >
            {listing && (
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
            )}
          </Marker>
        );
      })}
    </>
  );
}

export default function MapClient({ 
  listings = [], 
  center = [1.3521, 103.8198],
  zoom = 12,
  className = "h-full w-full rounded-lg",
  singleListing = false,
  onBoundsChange,
}: MapClientProps) {
  const [activeId, setActiveId] = useState<number | null>(null);

  // Calculate effective center
  let mapCenter: [number, number] = center;
  if (singleListing && listings.length > 0) {
    const listing = listings[0];
    const seed = listing.id;
    const lat = listing.latitude ?? (1.3521 + Math.sin(seed) * 0.05);
    const lng = listing.longitude ?? (103.8198 + Math.cos(seed) * 0.05);
    mapCenter = [lat, lng];
  }

  if (singleListing && listings.length > 0) {
    // Single listing: just show the home icon, no clustering needed
    const listing = listings[0];
    const seed = listing.id;
    const lat = listing.latitude ?? (1.3521 + Math.sin(seed) * 0.05);
    const lng = listing.longitude ?? (103.8198 + Math.cos(seed) * 0.05);
    return (
      <MapContainer center={mapCenter} zoom={zoom} className={className} scrollWheelZoom={true}>
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
          url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
        />
        <MapController center={mapCenter} zoom={zoom} />
        <Marker position={[lat, lng]} icon={createHomeIcon(listing.price, listing.display_price)} zIndexOffset={1000}>
          <Popup>
            <div className="min-w-[200px]">
              <h3 className="font-semibold text-sm">{listing.title}</h3>
              <p className="text-xs text-muted-foreground truncate">{listing.address}</p>
              <div className="mt-2 font-bold text-primary">
                {listing.display_price || (listing.price ? `$${listing.price.toLocaleString()}` : "Price/Ask")}
              </div>
            </div>
          </Popup>
        </Marker>
      </MapContainer>
    );
  }

  return (
    <MapContainer center={mapCenter} zoom={zoom} className={className} scrollWheelZoom={true}>
      <TileLayer
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
        url="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png"
      />
      <MapController center={mapCenter} zoom={zoom} />
      <ClusterLayer
        listings={listings}
        singleListing={singleListing}
        activeId={activeId}
        onActiveChange={setActiveId}
        onBoundsChange={onBoundsChange}
      />
    </MapContainer>
  );
}
