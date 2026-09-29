// Package clients contains the order service's gRPC adapters for its
// dependencies.
//
// Each adapter implements a port declared in `internal/domain`, so the saga
// depends on an interface rather than on a generated client. The translation
// itself is mechanical — proto to domain and back — but two details matter:
//
//   - gRPC statuses are converted into the shared error taxonomy on the way in,
//     because the saga's retry policy classifies on that taxonomy. A raw status
//     error would be treated as unclassified and therefore permanent, so a
//     transient provider outage would trigger a rollback instead of a retry.
//   - The order service never assumes another service's key format or schema.
//     Identifiers are opaque strings and only the fields actually needed are
//     copied out.
package clients

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/marketplace/pkg/apperr"
	catalogv1 "github.com/marketplace/pkg/proto/catalog/v1"
	"github.com/marketplace/pkg/protox"

	"github.com/marketplace/services/order/internal/domain"
)

// CatalogClient implements domain.CatalogClient over gRPC.
type CatalogClient struct {
	client catalogv1.CatalogServiceClient
}

// NewCatalogClient wraps a connection to the catalog service.
func NewCatalogClient(conn grpc.ClientConnInterface) *CatalogClient {
	return &CatalogClient{client: catalogv1.NewCatalogServiceClient(conn)}
}

// GetProducts prices a cart.
//
// Products that do not exist are absent from the returned map rather than
// zero-valued, which is how the caller distinguishes "unknown product" from
// "free product". A zero-priced entry would silently price a missing item at
// nothing and sell it.
func (c *CatalogClient) GetProducts(ctx context.Context, productIDs []string) (map[string]domain.Product, error) {
	if len(productIDs) == 0 {
		return map[string]domain.Product{}, nil
	}

	resp, err := c.client.GetProductsBatch(ctx, &catalogv1.GetProductsBatchRequest{
		ProductIds: productIDs,
	})
	if err != nil {
		return nil, apperr.FromGRPC("catalog.GetProducts", err)
	}

	out := make(map[string]domain.Product, len(resp.GetProducts()))
	for id, p := range resp.GetProducts() {
		if p == nil {
			continue
		}

		price, err := prox.MoneyFromProto(p.GetPrice())
		if err != nil {
			return nil, fmt.Errorf("catalog.GetProducts: product %s: %w", id, err)
		}

		out[id] = domain.Product{
			ID:       p.GetId(),
			SKU:      p.GetSku(),
			Name:     p.GetName(),
			Price:    price,
			IsActive: p.GetActive(),
		}
	}

	return out, nil
}
