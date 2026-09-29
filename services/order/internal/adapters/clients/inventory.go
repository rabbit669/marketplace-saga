package clients

import (
	"context"

	"google.golang.org/grpc"

	"github.com/marketplace/pkg/apperr"
	inventoryv1 "github.com/marketplace/pkg/proto/inventory/v1"
	"github.com/marketplace/pkg/protox"

	"github.com/marketplace/services/order/internal/domain"
)

// InventoryClient implements domain.InventoryClient over gRPC.
type InventoryClient struct {
	client inventoryv1.InventoryServiceClient
}

// NewInventoryClient wraps a connection to the inventory service.
func NewInventoryClient(conn grpc.ClientConnInterface) *InventoryClient {
	return &InventoryClient{client: inventoryv1.NewInventoryServiceClient(conn)}
}

// Reserve holds stock for an order.
//
// The reservation id is passed through unchanged, which is what makes the call
// idempotent: the caller supplies the order id, so a retried step returns the
// existing hold rather than taking a second one. The adapter does not generate
// an id here — generating one would silently destroy that property, and nothing
// downstream would notice until a customer could not buy anything else.
func (c *InventoryClient) Reserve(ctx context.Context, req domain.ReserveRequest) (domain.ReserveResult, error) {
	lines := make([]*inventoryv1.ReservationLine, 0, len(req.Lines))
	for _, l := range req.Lines {
		lines = append(lines, &inventoryv1.ReservationLine{
			ProductId: l.ProductID,
			Quantity:  l.Quantity,
		})
	}

	resp, err := c.client.ReserveStock(ctx, &inventoryv1.ReserveStockRequest{
		ReservationId: req.ReservationID,
		OrderId:       req.OrderID,
		Lines:         lines,
		TtlSeconds:    int64(req.TTL.Seconds()),
	})
	if err != nil {
		return domain.ReserveResult{}, apperr.FromGRPC("inventory.Reserve", err)
	}

	reservation := resp.GetReservation()
	if reservation == nil {
		// A successful response without a reservation would leave the caller
		// unable to release it later, so an unusable hold is worse than an
		// error: it must not be recorded as success.
		return domain.ReserveResult{}, apperr.Internal("inventory.Reserve",
			"inventory returned success without a reservation")
	}

	return domain.ReserveResult{
		ReservationID:  reservation.GetReservationId(),
		AlreadyExisted: resp.GetAlreadyExisted(),
		ExpiresAt:      prox.TimeFromProto(reservation.GetExpiresAt()),
	}, nil
}

// Commit turns a hold into a permanent deduction.
//
// This step has no compensation, so a failure here is retried by the saga's
// recovery worker rather than rolled back. The inventory service's commit is
// idempotent for exactly that reason.
func (c *InventoryClient) Commit(ctx context.Context, reservationID string) error {
	_, err := c.client.CommitReservation(ctx, &inventoryv1.CommitReservationRequest{
		ReservationId: reservationID,
	})
	if err != nil {
		return apperr.FromGRPC("inventory.Commit", err)
	}
	return nil
}

// Release returns a hold to the pool.
//
// It is called as a saga compensation, so it runs precisely when the caller does
// not know whether the reservation was ever created. The inventory service
// treats a missing or already-released reservation as success for that reason,
// and this adapter passes that through rather than inventing an error.
func (c *InventoryClient) Release(ctx context.Context, reservationID, reason string) error {
	_, err := c.client.ReleaseReservation(ctx, &inventoryv1.ReleaseReservationRequest{
		ReservationId: reservationID,
		Reason:        reason,
	})
	if err != nil {
		return apperr.FromGRPC("inventory.Release", err)
	}
	return nil
}
