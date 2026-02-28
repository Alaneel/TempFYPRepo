import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  images: {
    remotePatterns: [
      {
        protocol: "https",
        hostname: "images.unsplash.com",
      },
      {
        protocol: "https",
        hostname: "img.tepcdn.com", // In case Edgeprop images ever need it
      },
    ],
  },
};

export default nextConfig;
