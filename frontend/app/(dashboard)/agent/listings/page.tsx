"use client";

import { useQuery } from "@tanstack/react-query";
import api from "@/lib/api";
import { Listing, PaginatedResponse } from "@/types";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import Link from "next/link";
import { Plus, Pencil, Trash2, ExternalLink } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

export default function AgentListingsPage() {
  const { data, isLoading } = useQuery<PaginatedResponse<Listing>>({
    queryKey: ["agent-listings"],
    queryFn: async () => {
      // Fetch "My Listings" from the backend endpoint
      const response = await api.get("/listings/me/all", { params: { limit: 100 } });
      return response.data;
    },
  });

  // Hack for demo: Filter only listings where agent_id matches (if we had it).
  // For now, just show all to demonstrate the UI.
  
  return (
    <div className="space-y-6">
       <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold tracking-tight">My Listings</h1>
          <Button asChild>
             <Link href="/agent/listings/new">
                <Plus className="mr-2 h-4 w-4" /> Create Listing
             </Link>
          </Button>
       </div>

       <div className="border rounded-lg shadow-sm bg-background">
          <Table>
             <TableHeader>
                <TableRow>
                   <TableHead>Listing</TableHead>
                   <TableHead>Type</TableHead>
                   <TableHead>Price</TableHead>
                   <TableHead>Status</TableHead>
                   <TableHead className="text-right">Actions</TableHead>
                </TableRow>
             </TableHeader>
             <TableBody>
                {isLoading ? (
                    Array.from({ length: 5 }).map((_, i) => (
                       <TableRow key={i}>
                          <TableCell><Skeleton className="h-4 w-[200px]" /></TableCell>
                          <TableCell><Skeleton className="h-4 w-[80px]" /></TableCell>
                          <TableCell><Skeleton className="h-4 w-[100px]" /></TableCell>
                          <TableCell><Skeleton className="h-4 w-[60px]" /></TableCell>
                          <TableCell><Skeleton className="h-8 w-[100px] ml-auto" /></TableCell>
                       </TableRow>
                    ))
                ) : (
                    data?.data.map((listing) => (
                       <TableRow key={listing.id}>
                          <TableCell className="font-medium">
                             <div className="flex flex-col">
                                <span>{listing.title}</span>
                                <span className="text-xs text-muted-foreground">{listing.address}</span>
                             </div>
                          </TableCell>
                          <TableCell>{listing.property_type}</TableCell>
                          <TableCell>{new Intl.NumberFormat('en-SG', { style: 'currency', currency: 'SGD' }).format(listing.price || 0)}</TableCell>
                          <TableCell><Badge variant="secondary">Active</Badge></TableCell>
                          <TableCell className="text-right space-x-2">
                             <Button variant="ghost" size="icon" asChild>
                                <Link href={`/listings/${listing.id}`}>
                                   <ExternalLink className="h-4 w-4" />
                                </Link>
                             </Button>
                             <Button variant="ghost" size="icon">
                                <Pencil className="h-4 w-4" />
                             </Button>
                             <Button variant="ghost" size="icon" className="text-red-500 hover:text-red-600">
                                <Trash2 className="h-4 w-4" />
                             </Button>
                          </TableCell>
                       </TableRow>
                    ))
                )}
             </TableBody>
          </Table>
       </div>
    </div>
  );
}
