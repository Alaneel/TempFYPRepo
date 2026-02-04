"use client";

import { useAuth } from "@/lib/auth-context";
import { Button } from "@/components/ui/button";
import { LayoutDashboard, List, PlusCircle, LogOut, Settings, User, Upload } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";

export function DashboardSidebar() {
  const { user, logout } = useAuth();
  const pathname = usePathname();

  const routes = user?.role === 'admin' 
    ? [
        { href: "/admin/stats", label: "Overview", icon: LayoutDashboard },
        { href: "/admin/users", label: "Users", icon: User },
        { href: "/admin/listings/import", label: "Import", icon: Upload },
      ]
    : [
        { href: "/agent/listings", label: "My Listings", icon: List },
        { href: "/agent/listings/new", label: "Create Listing", icon: PlusCircle },
      ];

  return (
    <div className="hidden border-r bg-muted/40 md:block md:w-64 min-h-screen">
      <div className="flex h-full max-h-screen flex-col gap-2">
        <div className="flex h-14 items-center border-b px-4 lg:h-[60px] lg:px-6">
          <Link href="/" className="flex items-center gap-2 font-semibold">
            <LayoutDashboard className="h-6 w-6" />
            <span>Dashboard</span>
          </Link>
        </div>
        <div className="flex-1">
          <nav className="grid items-start px-2 text-sm font-medium lg:px-4">
             {routes.map((route) => (
                <Link
                  key={route.href}
                  href={route.href}
                  className={cn(
                    "flex items-center gap-3 rounded-lg px-3 py-2 transition-all hover:text-primary",
                    pathname === route.href ? "bg-muted text-primary" : "text-muted-foreground"
                  )}
                >
                  <route.icon className="h-4 w-4" />
                  {route.label}
                </Link>
             ))}
          </nav>
        </div>
        <div className="mt-auto p-4 border-t">
            <div className="flex items-center gap-3 px-3 py-2">
                <div className="h-8 w-8 rounded-full bg-primary/20 flex items-center justify-center">
                    <span className="font-bold text-xs">{user?.full_name?.substring(0,2).toUpperCase()}</span>
                </div>
                <div className="flex flex-col overflow-hidden">
                    <span className="text-sm font-medium truncate">{user?.full_name}</span>
                    <span className="text-xs text-muted-foreground truncate">{user?.email}</span>
                </div>
            </div>
            <Button variant="outline" className="w-full mt-2 justify-start text-red-600" onClick={logout}>
                <LogOut className="mr-2 h-4 w-4" />
                Sign Out
            </Button>
        </div>
      </div>
    </div>
  );
}
