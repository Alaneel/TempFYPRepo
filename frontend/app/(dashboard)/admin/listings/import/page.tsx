"use client";

import { useState } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import * as z from "zod";
import api from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
  FormDescription,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { toast } from "sonner";
import { Upload, FileText, AlertCircle } from "lucide-react";
// @ts-ignore
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useRouter } from "next/navigation";

const formSchema = z.object({
  file: z.instanceof(FileList).refine((files) => files?.length === 1, "File is required")
      .refine((files) => files?.[0]?.type === "text/csv" || files?.[0]?.name.endsWith(".csv"), "File must be a CSV"),
});

export default function ImportListingsPage() {
  const [isLoading, setIsLoading] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const router = useRouter();

  const form = useForm<z.infer<typeof formSchema>>({
    resolver: zodResolver(formSchema),
  });

  // Handle file input manually since react-hook-form doesn't support file inputs natively well with Shadcn Input
  const fileRef = form.register("file");

  async function onSubmit(values: z.infer<typeof formSchema>) {
    setIsLoading(true);
    setErrors([]);
    const formData = new FormData();
    formData.append("file", values.file[0]);

    try {
      const response = await api.post("/admin/listings/import_csv", formData, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      });

      toast.success(response.data.message);
      
      if (response.data.errors && response.data.errors.length > 0) {
        setErrors(response.data.errors);
        toast.warning("Some rows failed to import. Check details below.");
      } else {
         form.reset();
      }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } catch (error: any) {
      console.error(error);
      toast.error(error.response?.data?.detail || "Failed to import CSV");
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <div className="space-y-6">
      <h1 className="text-3xl font-bold tracking-tight">Import Listings</h1>
      
      <div className="grid gap-6 md:grid-cols-2">
      <Card>
        <CardHeader>
          <CardTitle>Upload CSV</CardTitle>
        </CardHeader>
        <CardContent>
          <Form {...form}>
            <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-6">
              <FormField
                control={form.control}
                name="file"
                render={({ field: { value, onChange, ...fieldProps } }) => (
                  <FormItem>
                    <FormLabel>CSV File</FormLabel>
                    <FormControl>
                      <Input
                        {...fieldProps}
                        placeholder="Select CSV"
                        type="file"
                        accept=".csv"
                        onChange={(event) => {
                          onChange(event.target.files);
                        }}
                      />
                    </FormControl>
                    <FormDescription>
                       Upload a CSV file with columns: title, price, beds, baths, sqft, address, property_type, district, description.
                    </FormDescription>
                    <FormMessage />
                  </FormItem>
                )}
              />
              <Button type="submit" disabled={isLoading}>
                {isLoading ? (
                    <>
                        <Upload className="mr-2 h-4 w-4 animate-spin" />
                        Importing...
                    </>
                ) : (
                    <>
                        <Upload className="mr-2 h-4 w-4" />
                        Start Import
                    </>
                )}
              </Button>
            </form>
          </Form>
        </CardContent>
      </Card>

      <Card>
          <CardHeader>
              <CardTitle>Instructions</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
              <div className="flex items-start gap-2 text-sm text-muted-foreground">
                  <FileText className="h-4 w-4 mt-1 flex-shrink-0" />
                  <p>
                      Ensure your CSV file is formatted correctly. The first row must be the header row.
                  </p>
              </div>
              <div className="rounded-md bg-muted p-4">
                  <code className="text-xs">
                      title,price,address,beds,baths,sqft,property_type,description<br/>
                      title,price,address,beds,baths,sqft,property_type,description<br/>
                      &quot;Luxury Condo&quot;,1500000,&quot;123 Main St&quot;,3,2,1200,&quot;Condo&quot;,&quot;Beautiful unit...&quot;
                  </code>
              </div>
          </CardContent>
      </Card>
      </div>

      {errors.length > 0 && (
          <Alert variant="destructive">
              <AlertCircle className="h-4 w-4" />
              <AlertTitle>Import Errors</AlertTitle>
              <AlertDescription>
                  <ul className="list-disc pl-5 mt-2 space-y-1">
                      {errors.map((err, index) => (
                          <li key={index}>{err}</li>
                      ))}
                  </ul>
              </AlertDescription>
          </Alert>
      )}
    </div>
  );
}
