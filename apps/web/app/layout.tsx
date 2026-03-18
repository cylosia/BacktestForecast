import type { Metadata } from "next";
import { headers } from "next/headers";
import { Inter } from "next/font/google";
import { ClerkProvider } from "@clerk/nextjs";
import "./globals.css";

const inter = Inter({ subsets: ["latin"], display: "swap" });

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
  // NOTE: The nonce is propagated to ClerkProvider. For full CSP compliance
  // with Next.js hydration scripts, ensure next.config.ts sets experimental
  // headers or uses the nonce middleware pattern.
  const nonce = headersList.get("x-nonce") ?? undefined;

  return (
    <html lang="en" suppressHydrationWarning className={inter.className}>
      <body className="min-h-screen bg-background text-foreground antialiased">
        <ClerkProvider nonce={nonce}>{children}</ClerkProvider>
      </body>
    </html>
  );
}
