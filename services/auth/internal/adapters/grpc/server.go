package grpcserver

import (
	"context"
	"errors"

	authv1 "github.com/marketplace/pkg/proto/auth/v1"
	"github.com/marketplace/pkg/protox"

	"github.com/marketplace/services/auth/internal/app"
	"github.com/marketplace/services/auth/internal/domain"
)

// Server implements authv1.AuthServiceServer.
type Server struct {
	authv1.UnimplementedAuthServiceServer

	service *app.Service
}

// New builds the transport adapter.
func New(service *app.Service) (*Server, error) {
	if service == nil {
		return nil, errors.New("auth grpc server: service is required")
	}
	return &Server{service: service}, nil
}

// Register implements authv1.AuthServiceServer.
func (s *Server) Register(ctx context.Context, req *authv1.RegisterRequest) (*authv1.RegisterResponse, error) {
	user, err := s.service.Register(ctx, req.GetEmail(), req.GetPassword(), req.GetFullName())
	if err != nil {
		return nil, err
	}
	return &authv1.RegisterResponse{User: toProtoUser(user)}, nil
}

// Login implements authv1.AuthServiceServer.
func (s *Server) Login(ctx context.Context, req *authv1.LoginRequest) (*authv1.LoginResponse, error) {
	user, pair, err := s.service.Login(ctx, req.GetEmail(), req.GetPassword(), req.GetClient())
	if err != nil {
		return nil, err
	}
	return &authv1.LoginResponse{
		AccessToken:  pair.AccessToken,
		RefreshToken: pair.RefreshToken,
		ExpiresIn:    int64(pair.ExpiresIn.Seconds()),
		User:         toProtoUser(user),
	}, nil
}

// Refresh implements authv1.AuthServiceServer.
func (s *Server) Refresh(ctx context.Context, req *authv1.RefreshRequest) (*authv1.RefreshResponse, error) {
	pair, err := s.service.Refresh(ctx, req.GetRefreshToken())
	if err != nil {
		return nil, err
	}
	return &authv1.RefreshResponse{
		AccessToken:  pair.AccessToken,
		RefreshToken: pair.RefreshToken,
		ExpiresIn:    int64(pair.ExpiresIn.Seconds()),
	}, nil
}

// Logout implements authv1.AuthServiceServer.
//
// Logging out revokes the family the presented token belongs to, so every
// descendant of that login stops working. Revoking only the presented token
// would leave the siblings a thief may already hold.
func (s *Server) Logout(ctx context.Context, req *authv1.LogoutRequest) (*authv1.LogoutResponse, error) {
	if err := s.service.Logout(ctx, req.GetRefreshToken()); err != nil {
		return nil, err
	}
	return &authv1.LogoutResponse{}, nil
}

// ValidateToken implements authv1.AuthServiceServer.
func (s *Server) ValidateToken(ctx context.Context, req *authv1.ValidateTokenRequest) (*authv1.ValidateTokenResponse, error) {
	claims, err := s.service.ValidateToken(ctx, req.GetAccessToken())
	if err != nil {
		// An invalid token is reported in the response rather than as an error:
		// the gateway calls this on every request and needs to distinguish
		// "rejected" from "the auth service is down", which is exactly the
		// difference between a 401 and a 503.
		return &authv1.ValidateTokenResponse{Valid: false, Reason: "invalid"}, nil
	}

	return &authv1.ValidateTokenResponse{
		Valid:  true,
		UserId: claims.Subject,
		Email:  claims.Email,
		Roles:  toProtoRoles(claims.Roles),
	}, nil
}

// GetUser implements authv1.AuthServiceServer.
func (s *Server) GetUser(ctx context.Context, req *authv1.GetUserRequest) (*authv1.GetUserResponse, error) {
	user, err := s.service.GetUser(ctx, req.GetUserId())
	if err != nil {
		return nil, err
	}
	return &authv1.GetUserResponse{User: toProtoUser(user)}, nil
}

// ------------------------------------------------------------- conversion

func toProtoUser(u domain.User) *authv1.User {
	return &authv1.User{
		Id:            u.ID,
		Email:         u.Email,
		FullName:      u.FullName,
		Roles:         toProtoRoles(u.Roles),
		EmailVerified: u.EmailVerified,
		CreatedAt:     prox.TimeToProto(u.CreatedAt),
	}
}

// toProtoRoles maps role names onto the enum.
//
// An unrecognised role becomes UNSPECIFIED rather than being dropped silently:
// a role the owning service does not know about is a deployment-skew bug, and
// discarding it would turn that into a silent authorisation failure.
func toProtoRoles(roles []string) []authv1.Role {
	out := make([]authv1.Role, 0, len(roles))
	for _, r := range roles {
		switch r {
		case "customer":
			out = append(out, authv1.Role_ROLE_CUSTOMER)
		case "admin":
			out = append(out, authv1.Role_ROLE_ADMIN)
		default:
			out = append(out, authv1.Role_ROLE_UNSPECIFIED)
		}
	}
	return out
}
