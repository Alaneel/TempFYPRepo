"use client";

import { useQuery } from "@tanstack/react-query";
import api from "@/lib/api";
import { AdminStats } from "@/types";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Users, Building, FileText } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

export default function AdminStatsPage() {
  const { data: stats, isLoading, error } = useQuery<AdminStats>({
    queryKey: ["admin-stats"],
    queryFn: async () => {
      const response = await api.get("/admin/stats");
      return response.data;
    },
  });

  if (isLoading) {
    return (
      <div className="space-y-6">
        <h1 className="text-3xl font-bold tracking-tight">Dashboard Overview</h1>
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
          <Skeleton className="h-32" />
          <Skeleton className="h-32" />
          <Skeleton className="h-32" />
        </div>
        <Skeleton className="h-[400px]" />
      </div>
    );
  }

  if (error) {
    return <div className="text-red-500">Failed to load stats</div>;
  }

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold tracking-tight">Dashboard Overview</h1>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Users</CardTitle>
            <Users className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.total_users}</div>
            <p className="text-xs text-muted-foreground">
              Registered users on the platform
            </p>
          </CardContent>
        </Card>

        <Card>
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-sm font-medium">Total Listings</CardTitle>
            <Building className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold">{stats?.total_listings}</div>
            <p className="text-xs text-muted-foreground">
              Active property listings
            </p>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-7">
        <Card className="col-span-4">
          <CardHeader>
            <CardTitle>Listings by Type</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {stats?.listings_by_type && Object.entries(stats.listings_by_type).map(([type, count]) => (
                <div key={type} className="flex items-center">
                  <div className="flex-1 space-y-1">
                    <p className="text-sm font-medium leading-none capitalize">{type?.replace('_', ' ') || 'Unknown'}</p>
                    <div className="w-full bg-secondary h-2 rounded-full mt-2">
                        <div 
                            className="bg-primary h-2 rounded-full" 
                            style={{ width: `${(count / (stats.total_listings || 1)) * 100}%` }}
                        />
                    </div>
                  </div>
                  <div className="ml-4 font-medium">{count}</div>
                </div>
              ))}
              {(!stats?.listings_by_type || Object.keys(stats.listings_by_type).length === 0) && (
                <p className="text-sm text-muted-foreground">No listing data available.</p>
              )}
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
