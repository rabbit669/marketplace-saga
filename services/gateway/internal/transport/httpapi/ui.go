package httpapi

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/marketplace/pkg/apperr"
	catalogv1 "github.com/marketplace/pkg/proto/catalog/v1"
	commonv1 "github.com/marketplace/pkg/proto/common/v1"
)

// The demo storefront is embedded in the gateway binary rather than served from
// a second container.
//
// Three reasons, in order of weight. It removes a service: no nginx, no extra
// image, no second thing to health-check. It removes CORS: the page and the API
// share an origin, so there is no preflight and no wildcard header to get wrong.
// And it keeps the frontend honest about what it is — a way to look at the API,
// not a product.
//
// It is plain HTML, CSS and one JavaScript file with no build step. A bundler
// would add a toolchain and a lockfile to a repository whose point is the
// backend, and would make the storefront the hardest part of the project to
// change.
//
//go:embed static/*
var staticFiles embed.FS

// mountUI serves the storefront at the root.
//
// The files are served under /ui/ and the root redirects there, so that a
// mistyped API path still reaches the JSON error handler rather than returning
// index.html — which is the classic way a single-page app turns a 404 into a
// confusing 200.
func (s *Server) mountUI(mux *http.ServeMux) {
	sub, err := fs.Sub(staticFiles, "static")
	if err != nil {
		// Unreachable: the embed directive guarantees the directory exists.
		return
	}

	files := http.FileServer(http.FS(sub))

	mux.Handle("GET /ui/", http.StripPrefix("/ui/", noCache(files)))
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
}

// noCache disables caching for the UI.
//
// The storefront changes whenever the binary does, and a developer looking at a
// stale page after a rebuild is a confusing hour. API responses already carry
// no-store.
func noCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}

// ---------------------------------------------------------------- products

type productResponse struct {
	ID         string            `json:"id"`
	SKU        string            `json:"sku"`
	Name       string            `json:"name"`
	Price      moneyDTO          `json:"price"`
	CategoryID string            `json:"category_id,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// handleListProducts backs the storefront's product grid.
//
// It exists because a storefront has to list something. The public API was
// originally order-only, which was enough for a client that already knew what it
// wanted to buy and nothing at all for a page that has to show it.
func (s *Server) handleListProducts() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.catalog == nil {
			writeError(w, r, apperr.Unavailable("gateway", "the catalogue is not configured"))
			return
		}

		resp, err := s.catalog.ListProducts(r.Context(), &catalogv1.ListProductsRequest{
			Page:       &commonv1.PageRequest{PageSize: 50},
			OnlyActive: true,
		})
		if err != nil {
			writeError(w, r, err)
			return
		}

		products := make([]productResponse, 0, len(resp.GetProducts()))
		for _, p := range resp.GetProducts() {
			products = append(products, productResponse{
				ID:         p.GetId(),
				SKU:        p.GetSku(),
				Name:       p.GetName(),
				Price:      moneyFromProto(p.GetPrice()),
				CategoryID: p.GetCategoryId(),
				Attributes: p.GetAttributes(),
			})
		}

		writeJSON(w, r, http.StatusOK, map[string]any{
			"products": products,
			"count":    len(products),
		})
	})
}

// registerUIRoutes is called from Handler so the routes sit next to the rest.
func (s *Server) registerUIRoutes(mux *http.ServeMux, route func(http.Handler) http.Handler) {
	s.mountUI(mux)
	mux.Handle("GET /v1/products", route(s.handleListProducts()))
}
