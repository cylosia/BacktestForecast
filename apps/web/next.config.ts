import type { NextConfig } from "next";

const withBundleAnalyzer =
  process.env.ANALYZE === "true"
    ? require("@next/bundle-analyzer")({ enabled: true })
    : (config: NextConfig) => config;

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";

function buildFallbackCsp(): string {
  const directives = [
    `default-src 'self'`,
    // Fallback policy for any route that somehow misses middleware. Middleware
    // still overwrites this with a stricter nonce-based CSP on normal requests.
    `script-src 'self' 'unsafe-inline' https://*.clerk.accounts.dev https://*.clerk.dev https://*.clerk.com https://challenges.cloudflare.com`,
    `style-src 'self' 'unsafe-inline'`,
    `img-src 'self' data: https://*.clerk.com https://*.clerk.dev https://*.stripe.com`,
    `font-src 'self' data:`,
    `connect-src 'self' https://*.clerk.accounts.dev https://*.clerk.dev https://*.clerk.com https://challenges.cloudflare.com ${API_BASE_URL}`.trim(),
    `frame-src 'self' https://*.clerk.accounts.dev https://*.clerk.com https://*.stripe.com https://challenges.cloudflare.com`,
    `worker-src 'self' blob:`,
    `object-src 'none'`,
    `base-uri 'self'`,
    `form-action 'self'`,
    `frame-ancestors 'self'`,
    ...(process.env.NODE_ENV === "production" ? [`upgrade-insecure-requests`] : []),
  ];
  if (process.env.CSP_REPORT_URI) {
    directives.push(`report-uri ${process.env.CSP_REPORT_URI}`);
    directives.push(`report-to csp-endpoint`);
  }
  return directives.join("; ");
}

const FALLBACK_CSP = buildFallbackCsp();

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
          { key: "X-Frame-Options", value: "SAMEORIGIN" },
          { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
          { key: "Cross-Origin-Opener-Policy", value: "same-origin" },
          { key: "Cross-Origin-Embedder-Policy", value: "unsafe-none" },
          {
            key: "Content-Security-Policy",
            value: FALLBACK_CSP,
          },
          {
            key: "Permissions-Policy",
            value: "camera=(), microphone=(), geolocation=()",
          },
          {
            key: "Strict-Transport-Security",
            value: "max-age=63072000; includeSubDomains; preload",
          },
          // Middleware replaces the fallback CSP above with a stricter
          // per-request nonce-based policy for normal page requests.
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
