import { clerkMiddleware, createRouteMatcher } from "@clerk/nextjs/server";
import { NextResponse } from "next/server";
import { isClerkEnabled } from "@/lib/clerk";

const isProtectedRoute = createRouteMatcher(["/app(.*)"]);

const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";

if (API_BASE_URL.includes("*")) {
  console.warn(
    "[CSP] NEXT_PUBLIC_API_BASE_URL contains wildcard (*). CSP connect-src should not use wildcards for API URLs."
  );
}
if (API_BASE_URL) {
  try {
    new URL(API_BASE_URL);
  } catch {
    console.warn("[CSP] NEXT_PUBLIC_API_BASE_URL is not a valid URL:", API_BASE_URL);
  }
}
const CSP_REPORT_URI = process.env.CSP_REPORT_URI ?? "";

function buildCSP(nonce: string): string {
  const directives = [
    `default-src 'self'`,
    `script-src 'self' 'nonce-${nonce}' 'strict-dynamic' https://*.clerk.accounts.dev https://*.clerk.dev https://*.clerk.com https://challenges.cloudflare.com`,
    `style-src 'self' 'unsafe-inline'`,  // unsafe-inline required by Tailwind runtime + Clerk SDK injected styles
    `img-src 'self' data: https://*.clerk.com https://*.clerk.dev https://*.stripe.com`,
    `font-src 'self' data:`,
    `connect-src 'self' https://*.clerk.accounts.dev https://*.clerk.dev https://*.clerk.com https://challenges.cloudflare.com ${API_BASE_URL}`,
    `frame-src 'self' https://*.clerk.accounts.dev https://*.clerk.com https://*.stripe.com https://challenges.cloudflare.com`,
    `worker-src 'self' blob:`,
    `object-src 'none'`,
    `base-uri 'self'`,
    `form-action 'self'`,
    `frame-ancestors 'self'`,
    ...(process.env.NODE_ENV === "production" ? [`upgrade-insecure-requests`] : []),
  ];
  if (CSP_REPORT_URI) {
    directives.push(`report-uri ${CSP_REPORT_URI}`);
    directives.push(`report-to csp-endpoint`);
  }
  return directives.join("; ");
}

function buildResponse(req: Request): NextResponse {
  const nonce = Buffer.from(crypto.getRandomValues(new Uint8Array(16))).toString("base64");
  const csp = buildCSP(nonce);

  const requestHeaders = new Headers(req.headers);
  requestHeaders.set("x-nonce", nonce);

  const response = NextResponse.next({
    request: {
      headers: requestHeaders,
    },
  });

  response.headers.set("Content-Security-Policy", csp);

  return response;
}

const middleware = isClerkEnabled()
  ? clerkMiddleware(async (auth, req) => {
      if (isProtectedRoute(req)) {
        await auth.protect();
      }
      return buildResponse(req);
    })
  : async function middlewareWithoutClerk(req: Request) {
      return buildResponse(req);
    };

export default middleware;

export const config = {
  matcher: [
    "/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)",
    "/(api|trpc)(.*)",
  ],
};
