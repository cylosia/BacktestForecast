import type { Metadata } from "next";
import { headers } from "next/headers";
import { Inter } from "next/font/google";
import { ClerkProvider } from "@clerk/nextjs";
import "./globals.css";

const inter = Inter({ subsets: ["latin"], display: "swap" });

import type { Viewport } from "next";

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
};

export const metadata: Metadata = {
  title: "BacktestForecast.com",
  description: "Historical options backtesting for retail traders.",
  openGraph: {
    title: "BacktestForecast.com",
    description: "Historical options backtesting for retail traders.",
    type: "website",
  },
};

export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const headersList = await headers();
  const nonce = headersList.get("x-nonce");
  if (!nonce) {
    throw new Error("Missing CSP nonce header. Ensure apps/web/middleware.ts runs for all rendered app routes.");
  }

  return (
    <html lang="en" suppressHydrationWarning className={inter.className}>
      <body className="min-h-screen bg-background text-foreground antialiased">
        <ClerkProvider nonce={nonce}>{children}</ClerkProvider>
      </body>
    </html>
  );
}
