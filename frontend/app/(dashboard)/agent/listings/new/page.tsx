"use client";

import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import * as z from "zod";
import api from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Checkbox } from "@/components/ui/checkbox";
import { toast } from "sonner";
import { useRouter } from "next/navigation";
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";

const formSchema = z.object({
  title: z.string().min(5, "Title must be at least 5 characters"),
  address: z.string().min(5, "Address must be at least 5 characters"),
  property_type: z.string(),
  buy_rent: z.enum(["Sale", "Rent"]),
  price: z.coerce.number().min(1, "Price is required"),
  beds: z.coerce.number().min(0),
  baths: z.coerce.number().min(0),
  sqft: z.coerce.number().min(1, "Sqft must be greater than 0"),
  district: z.coerce.number().min(1).max(28),
  description: z.string().optional(),
  built_year: z.string().optional(),
});

export default function CreateListingPage() {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const form = useForm<any>({
    // @ts-ignore
    resolver: zodResolver(formSchema),
    defaultValues: {
      title: "",
      address: "",
      property_type: "Condo",
      buy_rent: "Sale",
      price: 0,
      beds: 1,
      baths: 1,
      sqft: 500,
      district: 1,
      description: "",
      built_year: "",
    },
  });

  async function onSubmit(values: z.infer<typeof formSchema>) {
    setIsLoading(true);
    try {
      await api.post("/listings/", values);
      toast.success("Listing created", { description: "Your property is now live." });
      router.push("/agent/listings");
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
      console.error(error);
      const msg = error.response?.data?.detail || "Failed to create listing";
      toast.error("Error", { description: msg });
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="max-w-3xl mx-auto py-6">
       <div className="mb-6">
          <h1 className="text-2xl font-bold tracking-tight">Create New Listing</h1>
          <p className="text-muted-foreground">Fill in the details below to publish your property.</p>
       </div>
       
       <Card>
         <CardHeader>
            <CardTitle>Property Details</CardTitle>
         </CardHeader>
         <CardContent>
           <Form {...form}>
             {/* @ts-ignore */}
             <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
               <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <FormField
                    control={form.control}
                    name="title"
                    render={({ field }) => (
                      <FormItem className="col-span-2">
                        <FormLabel>Listing Title</FormLabel>
                        <FormControl>
                          <Input placeholder="e.g. Spacious 3BR Condo near MRT" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <FormField
                    control={form.control}
                    name="address"
                    render={({ field }) => (
                      <FormItem className="col-span-2">
                        <FormLabel>Address</FormLabel>
                        <FormControl>
                          <Input placeholder="e.g. 123 Orchard Road" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <FormField
                    control={form.control}
                    name="property_type"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Property Type</FormLabel>
                        <Select onValueChange={field.onChange} defaultValue={field.value}>
                          <FormControl>
                            <SelectTrigger>
                              <SelectValue placeholder="Select type" />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            <SelectItem value="Condo">Condo</SelectItem>
                            <SelectItem value="Landed">Landed</SelectItem>
                            <SelectItem value="HDB">HDB</SelectItem>
                            <SelectItem value="Commercial">Commercial</SelectItem>
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <FormField
                    control={form.control}
                    name="buy_rent"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Listing Mode</FormLabel>
                        <Select onValueChange={field.onChange} defaultValue={field.value}>
                          <FormControl>
                            <SelectTrigger>
                              <SelectValue placeholder="Select mode" />
                            </SelectTrigger>
                          </FormControl>
                          <SelectContent>
                            <SelectItem value="Sale">For Sale</SelectItem>
                            <SelectItem value="Rent">For Rent</SelectItem>
                          </SelectContent>
                        </Select>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <FormField
                    control={form.control}
                    name="price"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Price (SGD)</FormLabel>
                        <FormControl>
                          <Input type="number" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <FormField
                    control={form.control}
                    name="sqft"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>Floor Area (sqft)</FormLabel>
                        <FormControl>
                          <Input type="number" {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <div className="grid grid-cols-2 gap-4">
                      <FormField
                        control={form.control}
                        name="beds"
                        render={({ field }) => (
                          <FormItem>
                            <FormLabel>Beds</FormLabel>
                            <FormControl>
                              <Input type="number" {...field} />
                            </FormControl>
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                      <FormField
                        control={form.control}
                        name="baths"
                        render={({ field }) => (
                          <FormItem>
                            <FormLabel>Baths</FormLabel>
                            <FormControl>
                              <Input type="number" {...field} />
                            </FormControl>
                            <FormMessage />
                          </FormItem>
                        )}
                      />
                  </div>
                  
                  <FormField
                    control={form.control}
                    name="district"
                    render={({ field }) => (
                      <FormItem>
                        <FormLabel>District (1-28)</FormLabel>
                        <FormControl>
                          <Input type="number" min={1} max={28} {...field} />
                        </FormControl>
                        <FormMessage />
                      </FormItem>
                    )}
                  />
                  
                  <FormField
                    control={form.control}
                    name="built_year"
                    render={({ field }) => (
                      <FormItem>
                         <FormLabel>Built Year</FormLabel>
                         <FormControl>
                            <Input placeholder="e.g. 2015" {...field} />
                         </FormControl>
                         <FormMessage />
                      </FormItem>
                    )}
                  />
               </div>
               
               <Separator />
               
               <FormField
                  control={form.control}
                  name="description"
                  render={({ field }) => (
                    <FormItem>
                      <FormLabel>Description</FormLabel>
                      <FormControl>
                        <Textarea 
                          placeholder="Describe the property features, view, and amenities..." 
                          className="min-h-[150px]"
                          {...field} 
                        />
                      </FormControl>
                      <FormMessage />
                    </FormItem>
                  )}
                />

               <div className="flex justify-end gap-4">
                  <Button variant="outline" type="button" onClick={() => router.back()}>Cancel</Button>
                  <Button type="submit" disabled={isLoading}>
                    {isLoading ? "Publishing..." : "Publish Listing"}
                  </Button>
               </div>
             </form>
           </Form>
         </CardContent>
       </Card>
    </div>
  );
}
