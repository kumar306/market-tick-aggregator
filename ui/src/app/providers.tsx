"use client";

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useState } from "react";

// wrap in use state so new query client is not created on every render
export function Providers({ children }: { children: React.ReactNode }) {
    const [queryClient] = useState(
        () => 
            new QueryClient({
                defaultOptions: {
                    queries: {
                        retry: 1,
                        refetchOnWindowFocus: false
                    }
                }
            })
    )

    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}