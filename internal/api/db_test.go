package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetAllProducts(t *testing.T) {
	db := setupTestDB(t)
	products, err := GetAllProducts(db)
	require.NoError(t, err)
	assert.Len(t, products, 3)
	assert.Equal(t, "Coke", *products[0].Name)
	assert.Equal(t, "Burger", *products[1].Name)
	assert.Equal(t, "Fries", *products[2].Name)
}

func TestGetAllProducts_DBError(t *testing.T) {
	db := setupTestDB(t)
	db.Close()
	_, err := GetAllProducts(db)
	assert.Error(t, err)
}

func TestGetProductByID(t *testing.T) {
	tests := []struct {
		name        string
		id          string
		closeDB     bool
		expectedErr bool
		found       bool
	}{
		{
			name:  "ExistingProduct",
			id:    "PROD1",
			found: true,
		},
		{
			name:  "NonExistentProduct",
			id:    "NONEXISTENT",
			found: false,
		},
		{
			name:        "DBError",
			id:          "PROD1",
			closeDB:     true,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupTestDB(t)
			if tt.closeDB {
				db.Close()
			}
			p, err := GetProductByID(db, tt.id)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.found {
					assert.NotNil(t, p)
					assert.Equal(t, tt.id, *p.Id)
				} else {
					assert.Nil(t, p)
				}
			}
		})
	}
}

func TestGetProductsByIDs(t *testing.T) {
	tests := []struct {
		name          string
		ids           []string
		closeDB       bool
		expectedErr   bool
		expectedCount int
	}{
		{
			name:          "MultipleExisting",
			ids:           []string{"PROD1", "PROD2"},
			expectedCount: 2,
		},
		{
			name:          "MixedExistingAndNonExistent",
			ids:           []string{"PROD1", "NONEXISTENT"},
			expectedCount: 1,
		},
		{
			name:          "EmptyList",
			ids:           []string{},
			expectedCount: 0,
		},
		{
			name:        "DBError",
			ids:         []string{"PROD1"},
			closeDB:     true,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupTestDB(t)
			if tt.closeDB {
				db.Close()
			}
			products, err := GetProductsByIDs(db, tt.ids)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, products, tt.expectedCount)
			}
		})
	}
}

func TestCreateOrder(t *testing.T) {
	db := setupTestDB(t)
	coupon := "SAVE10"
	items := []OrderItem{
		{ProductID: "PROD1", Quantity: 2},
		{ProductID: "PROD2", Quantity: 1},
	}

	orderID, err := CreateOrder(db, &coupon, items)
	require.NoError(t, err)
	assert.NotEmpty(t, orderID)

	// Verify order exists
	var dbCoupon string
	err = db.QueryRow("SELECT coupon_code FROM orders WHERE id = ?", orderID).Scan(&dbCoupon)
	require.NoError(t, err)
	assert.Equal(t, coupon, dbCoupon)

	// Verify items exist
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM order_items WHERE order_id = ?", orderID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestCreateOrder_DBError(t *testing.T) {
	db := setupTestDB(t)
	db.Close()
	coupon := "SAVE10"
	items := []OrderItem{{ProductID: "PROD1", Quantity: 1}}
	_, err := CreateOrder(db, &coupon, items)
	assert.Error(t, err)
}

func TestValidateProductsExist(t *testing.T) {
	tests := []struct {
		name        string
		productIDs  []string
		closeDB     bool
		expectedErr bool
	}{
		{
			name:        "AllExist",
			productIDs:  []string{"PROD1", "PROD2"},
			expectedErr: false,
		},
		{
			name:        "OneMissing",
			productIDs:  []string{"PROD1", "NONEXISTENT"},
			expectedErr: true,
		},
		{
			name:        "EmptyList",
			productIDs:  []string{},
			expectedErr: true, // Function returns error for empty list
		},
		{
			name:        "DBError",
			productIDs:  []string{"PROD1"},
			closeDB:     true,
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := setupTestDB(t)
			if tt.closeDB {
				db.Close()
			}
			err := ValidateProductsExist(db, tt.productIDs)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestInitDB_Error(t *testing.T) {
	// Try to open a database in a non-existent directory
	_, err := InitDB("/non/existent/path/test.db")
	assert.Error(t, err)
}
