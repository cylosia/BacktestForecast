import type { Metadata } from "next";
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

// TODO: Add dark mode toggle. Dark mode variants (dark:...) are defined
// throughout the codebase but no mechanism exists to activate them yet.
export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning className={inter.className}>
      <body className="min-h-screen bg-background text-foreground antialiased">
        <a
          href="#main-content"
          className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[100] focus:rounded-md focus:bg-background focus:px-4 focus:py-2 focus:text-sm focus:font-medium focus:shadow-lg focus:ring-2 focus:ring-ring"
        >
          Skip to content
        </a>
        <ClerkProvider>{children}</ClerkProvider>
      </body>
    </html>
  );
}
