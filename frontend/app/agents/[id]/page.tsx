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
import { User, Phone, MapPin, ExternalLink, MessageCircle, Star } from "lucide-react";
import { ListingCard } from "@/components/features/listings/listing-card";

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
           limit: 50 // Fetch enough listings
         } 
       });
       return response.data;
    },
    enabled: !!id,
  });

  const isLoading = isAgentLoading || isListingsLoading;

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

  return (
    <div className="min-h-screen bg-background">
      <Navbar />
      
      {/* Agent Profile Header */}
      <div className="bg-white border-b">
         <div className="container mx-auto px-4 py-12">
            <div className="flex flex-col md:flex-row gap-8 items-start">
               {/* Photo */}
               <div className="w-32 h-32 md:w-48 md:h-48 rounded-full overflow-hidden border-4 border-white shadow-lg bg-gray-100 shrink-0 mx-auto md:mx-0">
                  {agent.photo_url ? (
                    <img src={agent.photo_url} alt={agent.name} className="w-full h-full object-cover" />
                  ) : (
                    <User className="w-full h-full p-8 text-gray-300" />
                  )}
               </div>
               
               {/* Info */}
               <div className="flex-grow text-center md:text-left space-y-4">
                  <div>
                    <h1 className="text-3xl font-bold">{agent.name}</h1>
                    <div className="text-muted-foreground mt-1 flex items-center justify-center md:justify-start gap-2">
                       {agent.cea && <Badge variant="outline">CEA: {agent.cea}</Badge>}
                       <span className="text-sm">Real Estate Agent</span>
                       {agent.rating !== undefined && agent.rating !== null && (
                         <div className="flex items-center gap-1 ml-2 text-yellow-500">
                           <Star className="w-4 h-4 fill-current" />
                           <span className="font-medium">{agent.rating.toFixed(1)}</span>
                         </div>
                       )}
                    </div>
                  </div>
                  
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
        <h2 className="text-2xl font-bold mb-6">Active Listings ({listingsData?.total || 0})</h2>
        
        {listingsData && listingsData.data.length > 0 ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
              {listingsData.data.map((listing) => (
                <ListingCard key={listing.id} listing={listing} />
              ))}
            </div>
        ) : (
            <div className="text-center py-12 text-muted-foreground bg-muted/20 rounded-xl">
               No active listings at the moment.
            </div>
        )}
      </div>
    </div>
  );
}
