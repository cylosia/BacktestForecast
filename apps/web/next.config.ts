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
          { key: "Cross-Origin-Embedder-Policy", value: "credentialless" },
          {
            key: "Permissions-Policy",
            value: "camera=(), microphone=(), geolocation=()",
          },
          {
            key: "Strict-Transport-Security",
            value: "max-age=63072000; includeSubDomains; preload",
          },
          {
            key: "Content-Security-Policy",
            // 'unsafe-inline' in script-src is required for Next.js inline scripts
            // (e.g. __NEXT_DATA__). A nonce-based CSP would be preferable but requires
            // custom server middleware. This is a documented security tradeoff.
            // TODO: Replace 'unsafe-inline' with nonce-based CSP in a future sprint.
            // This requires a custom Next.js server or middleware that generates a
            // per-request nonce, injects it into the CSP header, and passes it to
            // <Script nonce={nonce} /> components. Until then, 'unsafe-inline'
            // remains necessary to avoid breaking inline scripts.
            value: `default-src 'self'; script-src 'self' 'unsafe-inline' https://*.clerk.accounts.dev https://*.clerk.dev https://*.clerk.com; style-src 'self' 'unsafe-inline'; img-src 'self' data: https://*.clerk.com https://*.clerk.dev https://*.stripe.com; font-src 'self' data:; connect-src 'self' https://*.clerk.accounts.dev https://*.clerk.dev https://*.clerk.com ${process.env.NEXT_PUBLIC_API_BASE_URL ?? ""}; frame-src 'self' https://*.clerk.accounts.dev https://*.clerk.com https://*.stripe.com; worker-src 'self' blob:; object-src 'none'; base-uri 'self'; form-action 'self'; frame-ancestors 'self'; upgrade-insecure-requests${process.env.CSP_REPORT_URI ? `; report-uri ${process.env.CSP_REPORT_URI}; report-to csp-endpoint` : ""}`.replace(/\s+/g, " ").trim(),
          },
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
