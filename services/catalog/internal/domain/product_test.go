package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/marketplace/pkg/apperr"
	"github.com/marketplace/pkg/money"

	"github.com/marketplace/services/catalog/internal/domain"
)

// validInput is the baseline every rejection test mutates, so that a case fails
// for the reason it names and not because some other field was also wrong.
func validInput() domain.NewProductInput {
	return domain.NewProductInput{
		ID:          "0192f0a1-0000-7000-8000-000000000001",
		SKU:         "KETTLE-01",
		Name:        "Stainless Steel Kettle",
		Description: "1.7 litre brushed steel kettle.",
		Price:       money.MustNew(3999, money.USD),
		CategoryID:  "kitchen",
		Attributes:  map[string]string{"colour": "steel"},
		CreatedAt:   time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC),
	}
}

func TestNewProduct_NormalisesSKUAndStartsOnSale(t *testing.T) {
	in := validInput()
	in.SKU = "  kettle-01 "

	product, err := domain.NewProduct(in)
	if err != nil {
		t.Fatalf("NewProduct: %v", err)
	}

	if product.SKU != "KETTLE-01" {
		t.Errorf("sku = %q, want %q: the stored SKU must be the normalised one", product.SKU, "KETTLE-01")
	}
	if !product.IsActive {
		t.Error("a newly created product must be on sale, otherwise a create request produces something nobody can buy")
	}
	if !product.CreatedAt.Equal(in.CreatedAt) || !product.UpdatedAt.Equal(in.CreatedAt) {
		t.Errorf("timestamps = %s/%s, want both %s", product.CreatedAt, product.UpdatedAt, in.CreatedAt)
	}
}

func TestNewProduct_DefaultsTheClockWhenTheCallerHasNone(t *testing.T) {
	in := validInput()
	in.CreatedAt = time.Time{}

	before := time.Now()
	product, err := domain.NewProduct(in)
	if err != nil {
		t.Fatalf("NewProduct: %v", err)
	}

	if product.CreatedAt.Before(before) || product.CreatedAt.After(time.Now()) {
		t.Errorf("created at %s, want a timestamp between %s and now", product.CreatedAt, before)
	}
}

func TestNewProduct_RejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*domain.NewProductInput)
		wantField string
	}{
		{
			name:      "missing id",
			mutate:    func(in *domain.NewProductInput) { in.ID = "   " },
			wantField: "id",
		},
		{
			name:      "missing sku",
			mutate:    func(in *domain.NewProductInput) { in.SKU = "  " },
			wantField: "sku",
		},
		{
			name:      "sku with an embedded space",
			mutate:    func(in *domain.NewProductInput) { in.SKU = "KETTLE 01" },
			wantField: "sku",
		},
		{
			name:      "sku longer than the limit",
			mutate:    func(in *domain.NewProductInput) { in.SKU = strings.Repeat("A", domain.MaxSKULength+1) },
			wantField: "sku",
		},
		{
			name:      "missing name",
			mutate:    func(in *domain.NewProductInput) { in.Name = "\t\n" },
			wantField: "name",
		},
		{
			name:      "name longer than the limit",
			mutate:    func(in *domain.NewProductInput) { in.Name = strings.Repeat("x", domain.MaxNameLength+1) },
			wantField: "name",
		},
		{
			name: "price that was never set",
			// The zero Money has no currency. Accepting it would put the product
			// on sale for nothing.
			mutate:    func(in *domain.NewProductInput) { in.Price = money.Money{} },
			wantField: "price",
		},
		{
			name:      "negative price",
			mutate:    func(in *domain.NewProductInput) { in.Price = money.MustNew(-1, money.USD) },
			wantField: "price",
		},
		{
			name: "too many attributes",
			mutate: func(in *domain.NewProductInput) {
				attrs := make(map[string]string, domain.MaxAttributeCount+1)
				for i := 0; i <= domain.MaxAttributeCount; i++ {
					attrs[string(rune('a'+i%26))+strings.Repeat("x", i)] = "v"
				}
				in.Attributes = attrs
			},
			wantField: "attributes",
		},
		{
			name:      "attribute with an empty key",
			mutate:    func(in *domain.NewProductInput) { in.Attributes = map[string]string{"  ": "steel"} },
			wantField: "attributes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			in := validInput()
			tc.mutate(&in)

			product, err := domain.NewProduct(in)
			if err == nil {
				t.Fatalf("NewProduct accepted %+v, want rejection", product)
			}
			if product != nil {
				t.Errorf("NewProduct returned a product alongside the error %v", err)
			}

			var validation *domain.ValidationError
			if !errors.As(err, &validation) {
				t.Fatalf("error %v is not a *domain.ValidationError", err)
			}
			if validation.Field != tc.wantField {
				t.Errorf("field = %q, want %q (%v)", validation.Field, tc.wantField, err)
			}

			// The transport classification is part of the contract: a bad request
			// must reach the gateway as a 400 and not as a 500.
			if code := apperr.CodeOf(domain.AsAppError(err)); code != apperr.CodeInvalidArgument {
				t.Errorf("apperr code = %s, want %s", code, apperr.CodeInvalidArgument)
			}
		})
	}
}

func TestNewProduct_AcceptsAZeroPrice(t *testing.T) {
	// A free product is legitimate — a gift with purchase, a trial item — and
	// only a negative price is nonsense. Rejecting zero would be a rule with no
	// business justification behind it.
	in := validInput()
	in.Price = money.Zero(money.USD)

	if _, err := domain.NewProduct(in); err != nil {
		t.Fatalf("NewProduct rejected a zero price: %v", err)
	}
}

func TestNormalizeSKU(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "trims and upcases", raw: "  widget-01 ", want: "WIDGET-01"},
		{name: "keeps allowed punctuation", raw: "a_b.c/d-1", want: "A_B.C/D-1"},
		{name: "empty", raw: "   ", wantErr: true},
		{name: "non ascii", raw: "КЕТТЛ-01", wantErr: true},
		{name: "internal whitespace", raw: "A B", wantErr: true},
		{name: "too long", raw: strings.Repeat("A", domain.MaxSKULength+1), wantErr: true},
		{name: "exactly at the limit", raw: strings.Repeat("A", domain.MaxSKULength), want: strings.Repeat("A", domain.MaxSKULength)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := domain.NormalizeSKU(tc.raw)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("NormalizeSKU(%q) = %q, want an error", tc.raw, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("NormalizeSKU(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Errorf("NormalizeSKU(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestRehydrate_RebuildsAStoredProduct(t *testing.T) {
	created, err := domain.NewProduct(validInput())
	if err != nil {
		t.Fatalf("NewProduct: %v", err)
	}

	stored := *created
	stored.SKU = "kettle-01" // a legacy row written before normalisation existed
	stored.IsActive = false  // an off-sale product must survive a round trip

	rebuilt, err := domain.Rehydrate(stored)
	if err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}

	if rebuilt.SKU != "KETTLE-01" {
		t.Errorf("sku = %q, want the normalised form", rebuilt.SKU)
	}
	if rebuilt.IsActive {
		t.Error("Rehydrate must preserve the sale state: an off-sale product is still a product")
	}

	// The rebuilt value must own its attributes, otherwise a caller editing them
	// would be editing the row it was handed.
	rebuilt.Attributes["colour"] = "rust"
	if stored.Attributes["colour"] != "steel" {
		t.Error("Rehydrate shares its attribute map with the value it was given")
	}
}

func TestRehydrate_RejectsACorruptedRow(t *testing.T) {
	valid, err := domain.NewProduct(validInput())
	if err != nil {
		t.Fatalf("NewProduct: %v", err)
	}

	tests := []struct {
		name    string
		corrupt func(*domain.Product)
	}{
		{
			name:    "no id",
			corrupt: func(p *domain.Product) { p.ID = "" },
		},
		{
			name:    "no name",
			corrupt: func(p *domain.Product) { p.Name = "" },
		},
		{
			name:    "no sku",
			corrupt: func(p *domain.Product) { p.SKU = " " },
		},
		{
			name:    "price with no currency",
			corrupt: func(p *domain.Product) { p.Price = money.Money{} },
		},
		{
			name:    "negative price",
			corrupt: func(p *domain.Product) { p.Price = money.MustNew(-500, money.USD) },
		},
		{
			name:    "no creation timestamp",
			corrupt: func(p *domain.Product) { p.CreatedAt = time.Time{} },
		},
		{
			name: "updated before created",
			corrupt: func(p *domain.Product) {
				p.UpdatedAt = p.CreatedAt.Add(-time.Hour)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := *valid
			tc.corrupt(&row)

			if _, err := domain.Rehydrate(row); err == nil {
				t.Fatal("Rehydrate accepted a corrupted row")
			} else if !errors.Is(err, domain.ErrInvalidProduct) {
				t.Errorf("error %v does not wrap ErrInvalidProduct", err)
			}
		})
	}
}

func TestProduct_CloneCopiesTheAttributeMap(t *testing.T) {
	product, err := domain.NewProduct(validInput())
	if err != nil {
		t.Fatalf("NewProduct: %v", err)
	}

	cloned := product.Clone()
	cloned.Attributes["colour"] = "rust"
	cloned.Name = "Something Else"

	if product.Attributes["colour"] != "steel" {
		t.Error("Clone shares the attribute map: mutating the copy changed the original")
	}
	if product.Name != "Stainless Steel Kettle" {
		t.Error("Clone shares scalar state with the original")
	}
}

func TestAsAppError_MapsEveryDomainFailureOntoATransportCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want apperr.Code
	}{
		{name: "missing product", err: domain.ErrProductNotFound, want: apperr.CodeNotFound},
		{name: "duplicate sku", err: domain.ErrDuplicateSKU, want: apperr.CodeAlreadyExists},
		{name: "duplicate id", err: domain.ErrDuplicateProduct, want: apperr.CodeAlreadyExists},
		{name: "invalid product", err: domain.ErrInvalidProduct, want: apperr.CodeInvalidArgument},
		{name: "bad request field", err: domain.Invalid("name", "is required"), want: apperr.CodeInvalidArgument},
		{name: "unclassified", err: errors.New("connection reset"), want: apperr.CodeInternal},
		{name: "wrapped missing product", err: errors.Join(domain.ErrProductNotFound, errors.New("context")), want: apperr.CodeNotFound},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := apperr.CodeOf(domain.AsAppError(tc.err)); got != tc.want {
				t.Errorf("code = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestAsAppError_KeepsFailingFieldInMetadata(t *testing.T) {
	mapped := domain.AsAppError(domain.Invalid("sku", "is required"))

	var appErr *apperr.Error
	if !errors.As(mapped, &appErr) {
		t.Fatalf("error %v is not an *apperr.Error", mapped)
	}
	if appErr.Meta["field"] != "sku" {
		t.Errorf("meta field = %q, want %q", appErr.Meta["field"], "sku")
	}
}

func TestAsAppError_IsNilSafe(t *testing.T) {
	if err := domain.AsAppError(nil); err != nil {
		t.Errorf("AsAppError(nil) = %v, want nil", err)
	}
}
