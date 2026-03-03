export interface User {
  id: number;
  email: string;
  full_name?: string;
  role: "customer" | "agent" | "admin";
  is_active: boolean;
  created_at: string;
}

export interface Listing {
  id: number;
  title: string;
  address?: string;
  street_name?: string;
  price?: number;
  display_price?: string;
  psf?: number;
  display_psf?: string;
  beds?: number;
  baths?: number;
  sqft?: number;
  built_year?: string;
  developer_name?: string;
  property_type?: string;
  tenure?: string;
  district?: number;
  neighbourhood?: string;
  amenities_json?: string;
  facilities_json?: string;
  description?: string;
  buy_rent?: string;
  has_swimming_pool?: boolean;
  has_gym?: boolean;
  image_url?: string;
  url?: string;
  agent_id: number;
  agent?: Agent;
  condo_id?: number;
  condo?: Condo;
  created_at: string;
  updated_at: string;
  match_score?: number;
  latitude?: number;
  longitude?: number;
}

export interface Agent {
  id: number;
  name: string;
  mobile?: string;
  cea?: string;
  description?: string;
  photo_url?: string;
  url?: string;
  rating?: number;
  // Fields from agent_list table
  company_name?: string;
  agency_license?: string;
  license_expiry?: string;
  registration_date?: string;
  listing_count?: number;
}

export interface Condo {
  id: number;
  condo_name?: string;
  developer_name?: string;
  street_name?: string;
  postal_code?: string;
  tenure?: string;
  total_units?: number;
  district?: number;
  mrt_nearby?: string;
  property_type?: string;
  top_date?: string;
  num_floors?: number;
  num_blocks?: number;
}

export interface PaginatedResponse<T> {
  total: number;
  page: number;
  limit: number;
  data: T[];
}

export interface AdminStats {
  total_users: number;
  total_listings: number;
  listings_by_type: Record<string, number>;
  generated_at: string;
}

export interface UserResponse {
  id: number;
  email: string;
  full_name: string;
  role: 'customer' | 'agent' | 'admin';
  is_active: boolean;
  created_at: string;
}
