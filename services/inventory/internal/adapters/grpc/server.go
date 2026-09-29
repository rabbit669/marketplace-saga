// Package grpc exposes the inventory domain over the InventoryService API.
//
// This is the transport boundary and nothing else: the handlers validate what
// only the transport can see (a negative ttl in a request field), translate
// between protobuf messages and domain types, and hand the work to the
// repository. The rules about how much stock may be held and for how long live
// in `domain`, which is why this file contains no arithmetic on quantities and
// no knowledge of what a reservation is for.
//
// Errors follow the same split. A handler returns whatever the domain returned;
// the shared grpcx error interceptor turns the *apperr.Error that
// domain.AsAppError produced into a gRPC status. No handler in this package
// imports `codes`, and none of them builds a status by hand.
package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	inventoryv1 "github.com/marketplace/pkg/proto/inventory/v1"

	"github.com/marketplace/services/inventory/internal/domain"
)

// Config carries the server's dependencies.
type Config struct {
	// Repository is where the stock actually lives.
	Repository domain.Repository
	// Logger receives one record per state-changing call. Optional.
	Logger *slog.Logger
	// DefaultTTL is applied to a ReserveStock request whose ttl_seconds is
	// zero. Zero falls back to domain.DefaultReservationTTL.
	DefaultTTL time.Duration
}

// Server implements inventoryv1.InventoryServiceServer.
//
// It embeds the generated Unimplemented implementation so that adding an RPC to
// the proto is a compile-time no-op here rather than a panic at runtime on a
// call this service has not been taught to answer.
type Server struct {
	inventoryv1.UnimplementedInventoryServiceServer

	repo       domain.Repository
	log        *slog.Logger
	defaultTTL time.Duration
}

// NewServer builds the adapter.
func NewServer(cfg Config) (*Server, error) {
	if cfg.Repository == nil {
		return nil, errors.New("inventory grpc: repository is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Server{repo: cfg.Repository, log: cfg.Logger, defaultTTL: cfg.DefaultTTL}, nil
}

// Register installs the service on a gRPC server.
func (s *Server) Register(reg grpc.ServiceRegistrar) {
	inventoryv1.RegisterInventoryServiceServer(reg, s)
}

// ------------------------------------------------------------------ stock

// GetStock reports stock levels.
//
// Products with no stock row are omitted, per the proto's contract. The
// mapping is done here and not in the domain because "absent" is a wire-level
// representation: the repository has already dropped the unknown ids, which is
// the same decision expressed in Go.
func (s *Server) GetStock(ctx context.Context, req *inventoryv1.GetStockRequest) (*inventoryv1.GetStockResponse, error) {
	if err := validateProductIDs(req.GetProductIds()); err != nil {
		return nil, domain.AsAppError(err)
	}

	items, err := s.repo.GetStock(ctx, req.GetProductIds())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	out := make([]*inventoryv1.StockItem, 0, len(items))
	for _, item := range items {
		out = append(out, stockItemToProto(item))
	}
	return &inventoryv1.GetStockResponse{Items: out}, nil
}

// SetStock sets an absolute on-hand quantity.
//
// A negative quantity is rejected by the domain rather than here, so that the
// rule holds for every caller of the repository and not just for gRPC ones.
func (s *Server) SetStock(ctx context.Context, req *inventoryv1.SetStockRequest) (*inventoryv1.SetStockResponse, error) {
	item, err := s.repo.SetStock(ctx, req.GetProductId(), req.GetQuantity())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	// idempotency_key is deliberately not used as a replay cache: setting an
	// absolute quantity is already idempotent, so replaying it produces the
	// same row regardless. It is recorded so that an operator can correlate a
	// change with the tool that made it.
	s.log.InfoContext(ctx, "stock set",
		slog.String("product_id", item.ProductID),
		slog.Int64("quantity", item.Quantity),
		slog.Int64("reserved", item.Reserved),
		slog.String("idempotency_key", req.GetIdempotencyKey()),
	)

	return &inventoryv1.SetStockResponse{Item: stockItemToProto(item)}, nil
}

// ------------------------------------------------------------ reservations

// ReserveStock holds stock for an order.
//
// The response's already_existed flag is the only way a caller can tell a
// replay from a first execution, and it is surfaced rather than logged because
// the saga uses it to decide whether a compensation is owed.
func (s *Server) ReserveStock(ctx context.Context, req *inventoryv1.ReserveStockRequest) (*inventoryv1.ReserveStockResponse, error) {
	ttl, err := s.ttlFromProto(req.GetTtlSeconds())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	result, err := s.repo.Reserve(ctx, domain.ReserveRequest{
		ReservationID: req.GetReservationId(),
		OrderID:       req.GetOrderId(),
		Lines:         linesFromProto(req.GetLines()),
		TTL:           ttl,
	})
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	s.log.InfoContext(ctx, "stock reserved",
		slog.String("reservation_id", result.Reservation.ID),
		slog.String("order_id", result.Reservation.OrderID),
		slog.Int("lines", len(result.Reservation.Lines)),
		slog.Bool("already_existed", result.AlreadyExisted),
	)

	return &inventoryv1.ReserveStockResponse{
		Reservation:    reservationToProto(result.Reservation),
		AlreadyExisted: result.AlreadyExisted,
	}, nil
}

// CommitReservation converts a hold into a permanent deduction.
func (s *Server) CommitReservation(ctx context.Context, req *inventoryv1.CommitReservationRequest) (*inventoryv1.CommitReservationResponse, error) {
	res, err := s.repo.Commit(ctx, req.GetReservationId())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	s.log.InfoContext(ctx, "reservation committed",
		slog.String("reservation_id", res.ID),
		slog.String("order_id", res.OrderID),
	)

	return &inventoryv1.CommitReservationResponse{Reservation: reservationToProto(res)}, nil
}

// ReleaseReservation returns a hold to the pool.
//
// released=false is not an error: it means there was nothing to give back,
// which is the normal outcome when a compensation runs for a forward step that
// never applied. The reservation field is then absent, because reporting a
// status this service never observed would be worse than reporting nothing.
func (s *Server) ReleaseReservation(ctx context.Context, req *inventoryv1.ReleaseReservationRequest) (*inventoryv1.ReleaseReservationResponse, error) {
	result, err := s.repo.Release(ctx, req.GetReservationId(), req.GetReason())
	if err != nil {
		return nil, domain.AsAppError(err)
	}

	resp := &inventoryv1.ReleaseReservationResponse{Released: result.Released}
	if result.Reservation != nil {
		resp.Reservation = reservationToProto(*result.Reservation)
	}

	s.log.InfoContext(ctx, "reservation release handled",
		slog.String("reservation_id", req.GetReservationId()),
		slog.Bool("released", result.Released),
		slog.String("reason", req.GetReason()),
	)

	return resp, nil
}

// GetReservation reports a reservation's recorded state.
func (s *Server) GetReservation(ctx context.Context, req *inventoryv1.GetReservationRequest) (*inventoryv1.GetReservationResponse, error) {
	res, err := s.repo.GetReservation(ctx, req.GetReservationId())
	if err != nil {
		return nil, domain.AsAppError(err)
	}
	return &inventoryv1.GetReservationResponse{Reservation: reservationToProto(res)}, nil
}

// ------------------------------------------------------------- conversion

// ttlFromProto converts ttl_seconds into a duration.
//
// The cap is enforced here rather than in the domain because it is a limit on
// what a *caller* may ask for; the TTL an operator configures for the service
// is their decision and is not bounded by it. Zero means "the service default",
// which is why this returns a possibly-zero duration rather than substituting
// a value: the repository owns that fallback.
func (s *Server) ttlFromProto(seconds int64) (time.Duration, error) {
	if seconds < 0 {
		return 0, fmt.Errorf("%w: ttl_seconds must not be negative, got %d", domain.ErrInvalidRequest, seconds)
	}
	if seconds == 0 {
		return s.defaultTTL, nil
	}
	if seconds > int64(domain.MaxReservationTTL/time.Second) {
		return 0, fmt.Errorf("%w: ttl_seconds %d exceeds the maximum hold of %s",
			domain.ErrInvalidRequest, seconds, domain.MaxReservationTTL)
	}
	return time.Duration(seconds) * time.Second, nil
}

// validateProductIDs rejects blank ids.
//
// A blank id is always a caller bug — a mis-indexed slice, an unset field — and
// silently ignoring it would answer a question nobody asked while hiding the
// bug. The empty *list* is different, and valid: it means "everything".
func validateProductIDs(ids []string) error {
	for _, id := range ids {
		if strings.TrimSpace(id) == "" {
			return fmt.Errorf("%w: product_ids must not contain an empty id", domain.ErrInvalidRequest)
		}
	}
	return nil
}

func linesFromProto(lines []*inventoryv1.ReservationLine) []domain.ReservationLine {
	out := make([]domain.ReservationLine, 0, len(lines))
	for _, l := range lines {
		out = append(out, domain.ReservationLine{
			ProductID: l.GetProductId(),
			Quantity:  l.GetQuantity(),
		})
	}
	return out
}

func stockItemToProto(item domain.StockItem) *inventoryv1.StockItem {
	return &inventoryv1.StockItem{
		ProductId: item.ProductID,
		Quantity:  item.Quantity,
		Reserved:  item.Reserved,
		// Available is sent rather than left for the caller to subtract: the
		// clamp at zero is a rule, and a client that recomputed it would
		// reproduce that rule differently, or not at all.
		Available: item.Available(),
		UpdatedAt: timestamppb.New(item.UpdatedAt),
	}
}

func reservationToProto(res domain.Reservation) *inventoryv1.Reservation {
	lines := make([]*inventoryv1.ReservationLine, 0, len(res.Lines))
	for _, l := range res.Lines {
		lines = append(lines, &inventoryv1.ReservationLine{
			ProductId: l.ProductID,
			Quantity:  l.Quantity,
		})
	}

	return &inventoryv1.Reservation{
		ReservationId: res.ID,
		OrderId:       res.OrderID,
		Lines:         lines,
		Status:        statusToProto(res.Status),
		CreatedAt:     timestamppb.New(res.CreatedAt),
		ExpiresAt:     timestamppb.New(res.ExpiresAt),
	}
}

// statusToProto maps the domain's status onto the wire enum.
//
// The two are separate types on purpose: the proto enum is a compatibility
// promise to other services, the Go string is what the rules compare against,
// and a rename on either side should be a deliberate edit here rather than a
// silent reinterpretation. Every domain value is listed, so an unknown status
// can only mean a bug, and it maps to UNSPECIFIED rather than to a plausible
// wrong answer such as ACTIVE.
func statusToProto(status domain.ReservationStatus) inventoryv1.ReservationStatus {
	switch status {
	case domain.ReservationActive:
		return inventoryv1.ReservationStatus_RESERVATION_STATUS_ACTIVE
	case domain.ReservationCommitted:
		return inventoryv1.ReservationStatus_RESERVATION_STATUS_COMMITTED
	case domain.ReservationReleased:
		return inventoryv1.ReservationStatus_RESERVATION_STATUS_RELEASED
	case domain.ReservationExpired:
		return inventoryv1.ReservationStatus_RESERVATION_STATUS_EXPIRED
	default:
		return inventoryv1.ReservationStatus_RESERVATION_STATUS_UNSPECIFIED
	}
}
