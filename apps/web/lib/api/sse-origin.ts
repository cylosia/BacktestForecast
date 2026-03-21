import { env } from "@/lib/env";

export function getAllowedSseProxyOrigin(appUrl: string = env.appUrl): string {
  return new URL(appUrl).origin;
}

export function resolveSseProxyRequestOrigin(headers: Headers): string | null {
  const origin = headers.get("origin");
  if (origin) {
    try {
      return new URL(origin).origin;
    } catch {
      return null;
    }
  }

  const referer = headers.get("referer");
  if (!referer) {
    return null;
  }

  try {
    return new URL(referer).origin;
  } catch {
    return null;
  }
}

export function isAllowedSseProxyOrigin(headers: Headers, appUrl: string = env.appUrl): boolean {
  const candidateOrigin = resolveSseProxyRequestOrigin(headers);
  if (!candidateOrigin) {
    return false;
  }

  return candidateOrigin === getAllowedSseProxyOrigin(appUrl);
}
