"use client";

import Link from "next/link";
import { useAuth } from "@/lib/auth-context";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger
} from "@/components/ui/dropdown-menu";
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar";
import { Building2, Menu, User, LogOut, LayoutDashboard, Heart, Sparkles } from "lucide-react";
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet";
import { useState } from "react";
import { useRouter } from "next/navigation";

const NavLinks = () => (
  <>
    <Link href="/listings?buy_rent=Sale" className="text-sm font-medium transition-colors hover:text-primary">
      Buy
    </Link>
    <Link href="/listings?buy_rent=Rent" className="text-sm font-medium transition-colors hover:text-primary">
      Rent
    </Link>
    <Link href="/listings?property_type=Condo" className="text-sm font-medium transition-colors hover:text-primary">
      Condos
    </Link>
    <Link href="/listings?property_type=Landed" className="text-sm font-medium transition-colors hover:text-primary">
      Landed
    </Link>
    <Link href="/listings?property_type=HDB" className="text-sm font-medium transition-colors hover:text-primary">
      HDB
    </Link>
    <Link href="/valuate" className="text-sm font-medium transition-colors hover:text-primary">
      Valuate
    </Link>
    <Link href="/directory" className="text-sm font-medium transition-colors hover:text-primary">
      Directory
    </Link>
    <Link href="/shortlist" className="text-sm font-medium transition-colors hover:text-primary">
      Compare
    </Link>
  </>
);

export function Navbar() {
  const { user, logout, isAuthenticated } = useAuth();
  const [isOpen, setIsOpen] = useState(false);
  const router = useRouter();

  const handleLogout = async () => {
    await logout();
    router.push('/login');
  };

  return (
    <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
      <div className="container mx-auto px-4 flex h-16 items-center">
        <Sheet open={isOpen} onOpenChange={setIsOpen}>
          <SheetTrigger asChild>
            <Button variant="ghost" className="mr-2 px-0 text-base hover:bg-transparent focus-visible:bg-transparent focus-visible:ring-0 focus-visible:ring-offset-0 md:hidden">
              <Menu className="h-6 w-6" />
              <span className="sr-only">Toggle Menu</span>
            </Button>
          </SheetTrigger>
          <SheetContent side="left" className="pr-0">
            <Link href="/" className="flex items-center gap-2 font-bold text-xl mb-8" onClick={() => setIsOpen(false)}>
              <Building2 className="h-6 w-6 text-primary" />
              <span>SgEstate</span>
            </Link>
            <nav className="flex flex-col gap-4">
              <Link href="/listings?buy_rent=Sale" onClick={() => setIsOpen(false)} className="text-lg font-medium">Buy</Link>
              <Link href="/listings?buy_rent=Rent" onClick={() => setIsOpen(false)} className="text-lg font-medium">Rent</Link>
              <Link href="/agents" onClick={() => setIsOpen(false)} className="text-lg font-medium">Find Agent</Link>
              <Link href="/valuate" onClick={() => setIsOpen(false)} className="text-lg font-medium">Valuate</Link>
              <Link href="/directory" onClick={() => setIsOpen(false)} className="text-lg font-medium">Directory</Link>
              <Link href="/shortlist" onClick={() => setIsOpen(false)} className="text-lg font-medium text-rose-500">Compare</Link>
            </nav>
          </SheetContent>
        </Sheet>

        <Link href="/" className="mr-6 flex items-center space-x-2">
          <Building2 className="h-6 w-6 text-primary" />
          <span className="hidden font-bold sm:inline-block text-xl">
            SgEstate
          </span>
        </Link>

        <nav className="flex items-center space-x-6 text-sm font-medium hidden md:flex">
          <NavLinks />
        </nav>

        <div className="flex flex-1 items-center justify-end space-x-2">
          {isAuthenticated ? (
            <DropdownMenu>
              <DropdownMenuTrigger asChild>
                <Button variant="ghost" className="relative h-8 w-8 rounded-full">
                  <Avatar className="h-8 w-8">
                    <AvatarImage src="" alt={user?.full_name} />
                    <AvatarFallback>{user?.full_name?.charAt(0).toUpperCase() || "U"}</AvatarFallback>
                  </Avatar>
                </Button>
              </DropdownMenuTrigger>
              <DropdownMenuContent className="w-56" align="end" forceMount>
                <DropdownMenuLabel className="font-normal">
                  <div className="flex flex-col space-y-1">
                    <p className="text-sm font-medium leading-none">{user?.full_name}</p>
                    <p className="text-xs leading-none text-muted-foreground">
                      {user?.email}
                    </p>
                  </div>
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
                {user?.role === 'admin' && (
                  <DropdownMenuItem asChild>
                    <Link href="/admin/stats">
                      <LayoutDashboard className="mr-2 h-4 w-4" />
                      Admin Dashboard
                    </Link>
                  </DropdownMenuItem>
                )}
                {/* Note: I'm checking roles loosely here. Ideally constants. */}
                {(user?.role === 'agent') && (
                  <DropdownMenuItem asChild>
                    <Link href="/agent/listings">
                      <LayoutDashboard className="mr-2 h-4 w-4" />
                      Agent Dashboard
                    </Link>
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem asChild>
                  <Link href="/favourites">
                    <Heart className="mr-2 h-4 w-4 text-red-500" />
                    Saved Listings
                  </Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                  <Link href="/recommendations">
                    <Sparkles className="mr-2 h-4 w-4 text-primary" />
                    For You
                  </Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                  <Link href="/profile">
                    <User className="mr-2 h-4 w-4" />
                    Profile
                  </Link>
                </DropdownMenuItem>
                <DropdownMenuSeparator />
                <DropdownMenuItem onClick={handleLogout} className="text-red-600 focus:text-red-600">
                  <LogOut className="mr-2 h-4 w-4" />
                  Log out
                </DropdownMenuItem>
              </DropdownMenuContent>
            </DropdownMenu>
          ) : (
            <>
              <Button variant="ghost" asChild>
                <Link href="/login">Log In</Link>
              </Button>
              <Button asChild>
                <Link href="/register">Sign Up</Link>
              </Button>
            </>
          )}
        </div>
      </div>
    </header>
  );
}
