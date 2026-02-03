"use client";

import dynamic from "next/dynamic";
import { Skeleton } from "@/components/ui/skeleton";
import { Listing } from "@/types";

const MapClient = dynamic(
  () => import("@/components/features/map/map-client"),
  { 
    ssr: false,
    loading: () => <Skeleton className="h-full w-full rounded-lg" />
  }
);

interface MapViewProps {
  listings?: Listing[];
  center?: [number, number];
  zoom?: number;
  className?: string;
  singleListing?: boolean;
}

export default function MapView(props: MapViewProps) {
  return <MapClient {...props} />;
}
