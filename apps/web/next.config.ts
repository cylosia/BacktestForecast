import type { NextConfig } from "next";

const withBundleAnalyzer =
  process.env.ANALYZE === "true"
    ? // eslint-disable-next-line @typescript-eslint/no-require-imports
      require("@next/bundle-analyzer")({ enabled: true })
    : (config: NextConfig) => config;

const nextConfig: NextConfig = {
  reactStrictMode: true,
  output: "standalone",
  experimental: {
    optimizePackageImports: ["lucide-react"],
  },
  async headers() {
    return [
      {
        source: "/(.*)",
        headers: [
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "X-Frame-Options", value: "DENY" },
          { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
          { key: "Cross-Origin-Opener-Policy", value: "same-origin" },
          { key: "Cross-Origin-Embedder-Policy", value: "unsafe-none" },
          {
            key: "Permissions-Policy",
            value: "camera=(), microphone=(), geolocation=()",
          },
          {
            key: "Strict-Transport-Security",
            value: "max-age=63072000; includeSubDomains; preload",
          },
          // Content-Security-Policy is set per-request in middleware.ts with a
          // unique nonce. Do not duplicate it here — static headers cannot
          // include per-request nonces.
          ...(process.env.CSP_REPORT_URI
            ? [
                {
                  key: "Report-To",
                  value: JSON.stringify({
                    group: "csp-endpoint",
                    max_age: 86400,
                    endpoints: [{ url: process.env.CSP_REPORT_URI }],
                  }),
                },
              ]
            : []),
        ],
      },
    ];
  },
};

export default withBundleAnalyzer(nextConfig);
