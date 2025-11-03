# Field and Schema Extensions for Redshift

This document describes the extensions made to the `Field` and `Schema` types in `risingwave_common::catalog` to support Redshift table constraints and descriptions.

## Overview

The `Field` and `Schema` types have been extended with optional metadata fields that enable better integration with Redshift table creation, particularly for specifying constraints and documentation.

## New Field Properties

The `Field` struct now includes the following optional properties:

- `is_not_null: Option<bool>` - Indicates if the field has a NOT NULL constraint
- `is_primary_key: Option<bool>` - Indicates if the field is a primary key
- `foreign_key: Option<String>` - Foreign key reference (e.g., "other_table(column)")
- `description: Option<String>` - Optional description/comment for the field

## New Schema Properties

The `Schema` struct now includes:

- `description: Option<String>` - Optional description/comment for the schema/table

## Builder Pattern API

Convenient builder-style methods are available for setting these properties:

```rust
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

// Create a field with constraints
let id_field = Field::new("id", DataType::Int64)
    .with_not_null(true)
    .with_primary_key(true)
    .with_description("Unique identifier");

// Create a field with foreign key
let user_id_field = Field::new("user_id", DataType::Int64)
    .with_not_null(true)
    .with_foreign_key("users(id)")
    .with_description("Reference to users table");

// Create a schema with description
let schema = Schema::new(vec![id_field, user_id_field])
    .with_description("Orders table");
```

## Redshift Table Creation Example

Here's how these fields can be used in the Redshift connector to generate CREATE TABLE statements with constraints:

```rust
use risingwave_common::catalog::{Field, Schema};
use risingwave_common::types::DataType;

// Define a schema with full constraint information
let schema = Schema::new(vec![
    Field::new("order_id", DataType::Int64)
        .with_not_null(true)
        .with_primary_key(true)
        .with_description("Unique order identifier"),
    
    Field::new("customer_id", DataType::Int64)
        .with_not_null(true)
        .with_foreign_key("customers(id)")
        .with_description("Reference to customer"),
    
    Field::new("order_date", DataType::Date)
        .with_not_null(true)
        .with_description("Date the order was placed"),
    
    Field::new("total_amount", DataType::Decimal)
        .with_description("Total order amount"),
])
.with_description("Orders table with customer references");

// The Redshift connector can now use these fields to generate SQL like:
// 
// CREATE TABLE orders (
//     order_id BIGINT NOT NULL PRIMARY KEY,
//     customer_id BIGINT NOT NULL REFERENCES customers(id),
//     order_date DATE NOT NULL,
//     total_amount DECIMAL
// );
//
// COMMENT ON TABLE orders IS 'Orders table with customer references';
// COMMENT ON COLUMN orders.order_id IS 'Unique order identifier';
// COMMENT ON COLUMN orders.customer_id IS 'Reference to customer';
// COMMENT ON COLUMN orders.order_date IS 'Date the order was placed';
// COMMENT ON COLUMN orders.total_amount IS 'Total order amount';
```

## Backward Compatibility

All new fields are optional (`Option<T>`), ensuring complete backward compatibility:

- Existing code that doesn't use these fields will continue to work unchanged
- Fields created without these constraints will have `None` values
- Serialization and deserialization (protobuf) handles optional fields correctly

```rust
// Old code continues to work
let simple_field = Field::new("name", DataType::Varchar);
assert_eq!(simple_field.is_not_null, None);
assert_eq!(simple_field.is_primary_key, None);
assert_eq!(simple_field.foreign_key, None);
assert_eq!(simple_field.description, None);
```

## Protobuf Definition

The protobuf definition in `proto/plan_common.proto` has been updated:

```protobuf
message Field {
  data.DataType data_type = 1;
  string name = 2;
  optional bool is_not_null = 3;
  optional bool is_primary_key = 4;
  optional string foreign_key = 5;
  optional string description = 6;
}
```

## Future Work

These extensions lay the foundation for:

1. Enhanced Redshift table creation with full constraint support
2. Automatic generation of table and column comments
3. Better schema documentation and metadata management
4. Support for other database sinks that require similar metadata

## Testing

Tests have been added in `src/common/src/catalog/schema.rs` demonstrating:

- Builder pattern usage
- Serialization/deserialization roundtrip
- Backward compatibility
- Redshift table schema examples
