package auth

import (
	"context"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// JWTAuthFunc is a basic JWT validation interceptor function.
// For the purpose of this simulation, it just checks for a non-empty token.
func JWTAuthFunc(ctx context.Context) (context.Context, error) {
	token, err := auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}

	if token == "" {
		return nil, status.Error(codes.Unauthenticated, "invalid auth token")
	}

	// In a real application, we would parse and validate the JWT signature and claims here.
	return ctx, nil
}
