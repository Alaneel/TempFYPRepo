"use client";

import { useQuery } from "@tanstack/react-query";
import { useParams } from "next/navigation";
import Link from "next/link";
import api from "@/lib/api";
import { Agent, Listing, PaginatedResponse } from "@/types";
import { Navbar } from "@/components/layout/navbar";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { Card, CardContent } from "@/components/ui/card";
import { User, Phone, MapPin, ExternalLink, MessageCircle, Star, Building2, ShieldCheck, Calendar, BadgeCheck } from "lucide-react";
import { ListingCard } from "@/components/features/listings/listing-card";

// Backend base URL for photo URLs
const BACKEND_URL = process.env.NEXT_PUBLIC_API_URL?.replace('/api/v1', '') || 'http://localhost:8000';

export default function AgentDetailPage() {
  const { id } = useParams();

  // Fetch Agent Details
  const { data: agent, isLoading: isAgentLoading } = useQuery<Agent>({
    queryKey: ["agent", id],
    queryFn: async () => {
      const response = await api.get(`/agents/${id}`);
      return response.data;
    },
    enabled: !!id,
  });

  // Fetch Agent's Listings
  const { data: listingsData, isLoading: isListingsLoading } = useQuery<PaginatedResponse<Listing>>({
    queryKey: ["listings", "agent", id],
    queryFn: async () => {
       const response = await api.get("/listings/", { 
         params: { 
           agent_id: id,
           limit: 50
         } 
       });
       return response.data;
    },
    enabled: !!id,
  });

  const isLoading = isAgentLoading;

  // Helper to get full photo URL
  const getPhotoUrl = (photoUrl?: string) => {
    if (!photoUrl) return null;
    if (photoUrl.startsWith('http')) return photoUrl;
    return `${BACKEND_URL}${photoUrl}`;
  };

  // Format date helper
  const formatDate = (dateStr?: string) => {
    if (!dateStr) return null;
    try {
      return new Date(dateStr).toLocaleDateString('en-SG', {
        day: 'numeric',
        month: 'short',
        year: 'numeric'
      });
    } catch {
      return dateStr;
    }
  };

  if (isLoading) {
    return (
      <div className="min-h-screen bg-background">
        <Navbar />
        <div className="container mx-auto px-4 py-8 space-y-8">
           <Skeleton className="h-[200px] w-full rounded-xl" />
           <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              {[1, 2, 3, 4].map(i => <Skeleton key={i} className="h-[300px] w-full rounded-xl" />)}
           </div>
        </div>
      </div>
    );
  }

  if (!agent) {
      return (
        <div className="min-h-screen bg-background text-center py-20">
          <Navbar />
          <h1 className="text-2xl font-bold mt-10">Agent not found</h1>
          <Button className="mt-4" asChild><Link href="/agents">Find an Agent</Link></Button>
        </div>
      );
  }

  const photoUrl = getPhotoUrl(agent.photo_url);

  return (
    <div className="min-h-screen bg-background">
      <Navbar />
      
      {/* Agent Profile Header */}
      <div className="bg-white border-b">
         <div className="container mx-auto px-4 py-12">
            <div className="flex flex-col md:flex-row gap-8 items-start">
               {/* Photo */}
               <div className="w-32 h-32 md:w-48 md:h-48 rounded-full overflow-hidden border-4 border-white shadow-lg bg-gray-100 shrink-0 mx-auto md:mx-0">
                  {photoUrl ? (
                    <img src={photoUrl} alt={agent.name} className="w-full h-full object-cover" />
                  ) : (
                    <User className="w-full h-full p-8 text-gray-300" />
                  )}
               </div>
               
               {/* Info */}
               <div className="flex-grow text-center md:text-left space-y-4">
                  <div>
                    <h1 className="text-3xl font-bold">{agent.name}</h1>
                    <div className="text-muted-foreground mt-1 flex flex-wrap items-center justify-center md:justify-start gap-2">
                       {agent.cea && <Badge variant="outline">CEA: {agent.cea}</Badge>}
                       {agent.rating !== undefined && agent.rating !== null && (
                         <div className="flex items-center gap-1 text-yellow-500">
                           <Star className="w-4 h-4 fill-current" />
                           <span className="font-medium">{agent.rating.toFixed(1)}</span>
                         </div>
                       )}
                    </div>
                  </div>
                  
                  {/* Stats row */}
                  <div className="flex flex-wrap gap-4 text-sm justify-center md:justify-start">
                    <div className="flex items-center gap-1.5 font-medium">
                      <Building2 className="h-4 w-4 text-primary" />
                      <span>{agent.listing_count ?? listingsData?.total ?? 0} Active Listings</span>
                    </div>
                    {agent.registration_date && (
                      <div className="flex items-center gap-1.5 text-muted-foreground">
                        <BadgeCheck className="h-4 w-4 text-green-600" />
                        <span>Registered: {formatDate(agent.registration_date)}</span>
                      </div>
                    )}
                    {agent.license_expiry && (
                      <div className="flex items-center gap-1.5 text-muted-foreground">
                        <Calendar className="h-4 w-4" />
                        <span>License Expires: {formatDate(agent.license_expiry)}</span>
                      </div>
                    )}
                  </div>
                  
                  {/* Company Info Card */}
                  {agent.company_name && (
                    <Card className="inline-block">
                      <CardContent className="p-4 flex items-center gap-3">
                        <Building2 className="h-8 w-8 text-primary" />
                        <div>
                          <div className="font-semibold">{agent.company_name}</div>
                          {agent.agency_license && (
                            <div className="text-xs text-muted-foreground flex items-center gap-1">
                              <ShieldCheck className="h-3 w-3" />
                              License: {agent.agency_license}
                            </div>
                          )}
                        </div>
                      </CardContent>
                    </Card>
                  )}
                  
                  <p className="max-w-2xl text-muted-foreground whitespace-pre-line">
                     {agent.description || "Top performing agent specializing in residential properties."}
                  </p>
                  
                  <div className="flex flex-wrap gap-3 justify-center md:justify-start mt-4">
                     <Button asChild>
                        <a href={`tel:${agent.mobile || ""}`}>
                          <Phone className="mr-2 h-4 w-4" />
                          Call Agent
                        </a>
                     </Button>
                     <Button variant="outline" onClick={() => {
                        if (agent.mobile) {
                           window.open(`https://wa.me/65${agent.mobile.replace(/\D/g, '')}`, '_blank');
                        }
                     }}>
                        <MessageCircle className="mr-2 h-4 w-4" />
                        WhatsApp
                     </Button>
                     {agent.url && (
                        <Button variant="ghost" asChild>
                           <a href={agent.url} target="_blank" rel="noopener noreferrer">
                              <ExternalLink className="mr-2 h-4 w-4" />
                              Profile
                           </a>
                        </Button>
                     )}
                  </div>
               </div>
            </div>
         </div>
      </div>

      <div className="container mx-auto px-4 py-12">
        <h2 className="text-2xl font-bold mb-6">
          Active Listings ({agent.listing_count ?? listingsData?.total ?? 0})
        </h2>
        
        {isListingsLoading ? (
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
            {[1,2,3,4].map(i => <div key={i} className="h-80 bg-muted animate-pulse rounded-xl" />)}
          </div>
        ) : listingsData && listingsData.data.length > 0 ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
              {listingsData.data.map((listing) => (
                <ListingCard key={listing.id} listing={listing} />
              ))}
            </div>
        ) : (
            <div className="text-center py-16 text-muted-foreground bg-muted/20 rounded-xl space-y-2">
               <Building2 className="h-10 w-10 mx-auto text-muted-foreground/40" />
               <p className="font-medium">No active listings at the moment.</p>
               <p className="text-sm">Check back later for new properties from this agent.</p>
            </div>
        )}
      </div>
    </div>
  );
}
