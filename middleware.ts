// middleware.ts
import { NextResponse } from 'next/server'
import type { NextRequest } from 'next/server'

// Helper function to check if path is public
function isPublicPath(pathname: string): boolean {
  const publicPaths = [
    '/login',
    '/forgot-password',
    '/reset-password',
    '/training', // Training pages are public
    '/_next',
    '/favicon.ico',
    '/api/users/auth/login',
    '/api/users/auth/refresh'
  ];
  
  return publicPaths.some(path => pathname.startsWith(path));
}

// Helper function to check if path requires admin access
function requiresAdminAccess(pathname: string): boolean {
  const adminPaths = [
    '/admin',
    '/users',
    '/user-management'
  ];
  
  return adminPaths.some(path => pathname.startsWith(path));
}

// Helper function to get user from request (simplified - in production you'd verify JWT)
function getUserFromRequest(request: NextRequest) {
  try {
    // In a real implementation, you'd extract and verify the JWT token
    // For now, we'll check if user data exists in a cookie or header
    const userCookie = request.cookies.get('user');
    if (userCookie) {
      return JSON.parse(userCookie.value);
    }
    return null;
  } catch {
    return null;
  }
}

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Allow public paths
  if (isPublicPath(pathname)) {
    return NextResponse.next();
  }

  // Security headers for all responses
  const response = NextResponse.next();
  
  // Add security headers
  response.headers.set('X-Frame-Options', 'DENY');
  response.headers.set('X-Content-Type-Options', 'nosniff');
  response.headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');
  response.headers.set('X-XSS-Protection', '1; mode=block');
  
  // HTTPS enforcement (in production)
  if (process.env.NODE_ENV === 'production' && !request.nextUrl.protocol.includes('https')) {
    return NextResponse.redirect(`https://${request.nextUrl.host}${request.nextUrl.pathname}${request.nextUrl.search}`);
  }

  // Rate limiting could be implemented here
  // For now, we'll add basic request logging
  console.log(`${new Date().toISOString()} - ${request.method} ${pathname} - ${request.ip || 'unknown IP'}`);

  // Check authentication for protected routes
  const user = getUserFromRequest(request);
  
  if (!user) {
    // Redirect to login for unauthenticated users
    const loginUrl = new URL('/login', request.url);
    loginUrl.searchParams.set('redirect', pathname);
    return NextResponse.redirect(loginUrl);
  }

  // Check admin access for admin routes
  if (requiresAdminAccess(pathname)) {
    const adminRoles = ['system admin', 'administrator', 'admin'];
    const hasAdminAccess = adminRoles.some(role => 
      user.role?.toLowerCase().includes(role.toLowerCase())
    );
    
    if (!hasAdminAccess) {
      // Redirect non-admins to their default page
      return NextResponse.redirect(new URL('/dashboard', request.url));
    }
  }

  return response;
}

export const config = {
  matcher: [
    /*
     * Match all request paths except for the ones starting with:
     * - api (API routes)
     * - _next/static (static files)
     * - _next/image (image optimization files)
     * - favicon.ico (favicon file)
     */
    '/((?!api|_next/static|_next/image|favicon.ico).*)',
  ],
}
