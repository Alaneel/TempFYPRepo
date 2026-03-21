"use client";

import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Heart } from "lucide-react";

export function ShortlistButton({ listingId }: { listingId: number }) {
  const [isSaved, setIsSaved] = useState(false);

  useEffect(() => {
    const saved = JSON.parse(localStorage.getItem("property_shortlist") || "[]");
    setIsSaved(saved.includes(listingId));
  }, [listingId]);

  const toggleSave = () => {
    const saved: number[] = JSON.parse(localStorage.getItem("property_shortlist") || "[]");
    
    if (saved.includes(listingId)) {
      const newSaved = saved.filter(id => id !== listingId);
      localStorage.setItem("property_shortlist", JSON.stringify(newSaved));
      setIsSaved(false);
    } else {
      if (saved.length >= 3) {
        alert("You can only compare up to 3 properties at a time! Please remove one first. You can view your shortlist at /shortlist");
        return;
      }
      saved.push(listingId);
      localStorage.setItem("property_shortlist", JSON.stringify(saved));
      setIsSaved(true);
    }
  };

  return (
    <Button 
      variant={isSaved ? "default" : "outline"} 
      className={`w-full ${isSaved ? 'bg-rose-500 hover:bg-rose-600 text-white border-transparent' : 'border-rose-200 text-rose-600 hover:bg-rose-50'}`}
      onClick={toggleSave}
    >
      <Heart className={`mr-2 h-4 w-4 ${isSaved ? 'fill-current text-white' : ''}`} />
      {isSaved ? "Saved to Shortlist" : "Save to Compare"}
    </Button>
  );
}
