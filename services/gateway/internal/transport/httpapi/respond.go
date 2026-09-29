// Package httpapi is the API gateway's REST edge.
//
// The gateway is the only service published on the host network (ADR-0004). Its
// responsibilities are deliberately narrow and all of them are cross-cutting:
// verify the caller, limit their rate, translate REST into gRPC, and translate
// the answer back. It holds no business rules, which is why it can be replaced
// without touching anything that matters.
package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/marketplace/pkg/apperr"
)

// errorBody is the wire shape of every failure.
//
// It is stable and machine-readable on purpose: a client that has to parse an
// English sentence to decide whether to retry will get it wrong, and will get it
// wrong again the next time the sentence is edited.
type errorBody struct {
	Error struct {
		// Code is the transport-neutral classification, e.g. "not_found".
		Code string `json:"code"`
		// Message is safe to show a user. Internal failures carry a generic
		// message rather than a database error or a stack trace.
		Message string `json:"message"`
		// RequestID lets a support ticket be tied to a log line.
		RequestID string `json:"request_id,omitempty"`
		// Fields names the offending inputs for a validation failure, so a
		// client can highlight the right form field.
		Fields map[string]string `json:"fields,omitempty"`
	} `json:"error"`
}

// writeJSON encodes v as JSON with the given status.
func writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	// The response must not be cached: every endpoint here is either
	// authenticated or state-changing, and a shared cache holding one customer's
	// order is a data leak.
	w.Header().Set("Cache-Control", "no-store")

	w.WriteHeader(status)

	if v == nil {
		return
	}
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// The status line is already sent, so this cannot be turned into an
		// error response. Logging is all that is left.
		slog.ErrorContext(r.Context(), "could not encode response",
			"error", err.Error(),
			"path", r.URL.Path,
		)
	}
}

// writeError maps an error onto an HTTP response.
//
// This is the single translation point between the platform's error taxonomy and
// HTTP. Handlers return ordinary errors and never choose a status code, which is
// what keeps "404 means not found" true in every handler rather than in most of
// them.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	status := apperr.HTTPStatus(err)

	var body errorBody
	body.Error.Code = string(apperr.CodeOf(err))
	// PublicMessage returns "internal error" for anything unclassified, so an
	// implementation detail cannot escape through the API.
	body.Error.Message = apperr.PublicMessage(err)
	body.Error.RequestID = RequestIDFrom(r.Context())

	var appErr *apperr.Error
	if errors.As(err, &appErr) && len(appErr.Meta) > 0 {
		// The gateway only forwards meta it knows is safe. Field names are; free
		// form values may not be.
		if field, ok := appErr.Meta["field"]; ok {
			body.Error.Fields = map[string]string{"field": field}
		}
	}

	if status >= http.StatusInternalServerError {
		slog.ErrorContext(r.Context(), "request failed",
			"error", err.Error(),
			"code", body.Error.Code,
			"method", r.Method,
			"path", r.URL.Path,
			"request_id", body.Error.RequestID,
		)
	}

	writeJSON(w, r, status, body)
}

// decodeJSON reads a request body into v, rejecting anything unexpected.
//
// DisallowUnknownFields is deliberate. A client that misspells a field name
// would otherwise get a silently different operation — the most expensive class
// of API bug to diagnose — instead of an error naming the field.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	if r.Body == nil {
		return apperr.InvalidArgument("gateway", "a request body is required")
	}

	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()

	if err := dec.Decode(v); err != nil {
		return apperr.InvalidArgument("gateway", "the request body is not valid JSON: %s", err.Error())
	}

	// A second value in the stream means the client sent something other than a
	// single object, which is a mistake worth reporting rather than ignoring.
	if dec.More() {
		return apperr.InvalidArgument("gateway", "the request body must contain a single JSON object")
	}
	return nil
}

// maxBodyBytes bounds request bodies. Without a limit, one client can make the
// gateway allocate without bound, which is a denial of service that needs no
// exploit.
const maxBodyBytes = 1 << 20 // 1 MiB
