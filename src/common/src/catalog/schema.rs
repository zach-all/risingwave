// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Index;

use risingwave_pb::plan_common::{PbColumnDesc, PbField};

use super::ColumnDesc;
use crate::array::ArrayBuilderImpl;
use crate::types::{DataType, StructType};
use crate::util::iter_util::ZipEqFast;

/// The field in the schema of the executor's return data
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Field {
    pub data_type: DataType,
    pub name: String,
    /// Indicates if the field has a NOT NULL constraint
    pub is_not_null: Option<bool>,
    /// Indicates if the field is a primary key
    pub is_primary_key: Option<bool>,
    /// Foreign key reference (e.g., "other_table(column)")
    pub foreign_key: Option<String>,
    /// Optional description/comment for the field
    pub description: Option<String>,
}

impl Field {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            data_type,
            name: name.into(),
            is_not_null: None,
            is_primary_key: None,
            foreign_key: None,
            description: None,
        }
    }
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{:?}", self.name, self.data_type)
    }
}

impl Field {
    pub fn to_prost(&self) -> PbField {
        PbField {
            data_type: Some(self.data_type.to_protobuf()),
            name: self.name.clone(),
            is_not_null: self.is_not_null,
            is_primary_key: self.is_primary_key,
            foreign_key: self.foreign_key.clone(),
            description: self.description.clone(),
        }
    }

    pub fn from_prost(pb: &PbField) -> Self {
        Field {
            data_type: DataType::from(pb.data_type.as_ref().unwrap()),
            name: pb.name.clone(),
            is_not_null: pb.is_not_null,
            is_primary_key: pb.is_primary_key,
            foreign_key: pb.foreign_key.clone(),
            description: pb.description.clone(),
        }
    }
}

impl From<&ColumnDesc> for Field {
    fn from(desc: &ColumnDesc) -> Self {
        Self {
            data_type: desc.data_type.clone(),
            name: desc.name.clone(),
            is_not_null: None,
            is_primary_key: None,
            foreign_key: None,
            description: desc.description.clone(),
        }
    }
}

impl From<ColumnDesc> for Field {
    fn from(column_desc: ColumnDesc) -> Self {
        Self {
            data_type: column_desc.data_type,
            name: column_desc.name,
            is_not_null: None,
            is_primary_key: None,
            foreign_key: None,
            description: column_desc.description,
        }
    }
}

impl From<&PbColumnDesc> for Field {
    fn from(pb_column_desc: &PbColumnDesc) -> Self {
        Self {
            data_type: pb_column_desc.column_type.as_ref().unwrap().into(),
            name: pb_column_desc.name.clone(),
            is_not_null: None,
            is_primary_key: None,
            foreign_key: None,
            description: pb_column_desc.description.clone(),
        }
    }
}

/// Something that has a data type and a name.
#[auto_impl::auto_impl(&)]
pub trait FieldLike {
    fn data_type(&self) -> &DataType;
    fn name(&self) -> &str;
}

impl FieldLike for Field {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub struct FieldDisplay<'a>(pub &'a Field);

impl std::fmt::Debug for FieldDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name)
    }
}

impl std::fmt::Display for FieldDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.name)
    }
}

/// `schema_unnamed` builds a `Schema` with the given types, but without names.
#[macro_export]
macro_rules! schema_unnamed {
    ($($t:expr),*) => {{
        $crate::catalog::Schema {
            fields: vec![
                $( $crate::catalog::Field::unnamed($t) ),*
            ],
        }
    }};
}

/// the schema of the executor's return data
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Schema {
    pub fields: Vec<Field>,
    /// Optional description/comment for the schema
    pub description: Option<String>,
}

impl Schema {
    pub fn empty() -> &'static Self {
        static EMPTY: Schema = Schema { fields: Vec::new(), description: None };
        &EMPTY
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields, description: None }
    }

    /// Set the description for this schema
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn names(&self) -> Vec<String> {
        self.fields().iter().map(|f| f.name.clone()).collect()
    }

    pub fn names_str(&self) -> Vec<&str> {
        self.fields().iter().map(|f| f.name.as_str()).collect()
    }

    pub fn data_types(&self) -> Vec<DataType> {
        self.fields
            .iter()
            .map(|field| field.data_type.clone())
            .collect()
    }

    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    pub fn into_fields(self) -> Vec<Field> {
        self.fields
    }

    /// Create array builders for all fields in this schema.
    pub fn create_array_builders(&self, capacity: usize) -> Vec<ArrayBuilderImpl> {
        self.fields
            .iter()
            .map(|field| field.data_type.create_array_builder(capacity))
            .collect()
    }

    pub fn to_prost(&self) -> Vec<PbField> {
        self.fields
            .clone()
            .into_iter()
            .map(|field| field.to_prost())
            .collect()
    }

    pub fn type_eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for (a, b) in self.fields.iter().zip_eq_fast(other.fields.iter()) {
            if a.data_type != b.data_type {
                return false;
            }
        }

        true
    }

    pub fn all_type_eq<'a>(inputs: impl IntoIterator<Item = &'a Self>) -> bool {
        let mut iter = inputs.into_iter();
        if let Some(first) = iter.next() {
            iter.all(|x| x.type_eq(first))
        } else {
            true
        }
    }

    pub fn formatted_col_names(&self) -> String {
        self.fields
            .iter()
            .map(|f| format!("\"{}\"", &f.name))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl Field {
    // TODO: rename to `new`
    pub fn with_name<S>(data_type: DataType, name: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            data_type,
            name: name.into(),
            is_not_null: None,
            is_primary_key: None,
            foreign_key: None,
            description: None,
        }
    }

    pub fn unnamed(data_type: DataType) -> Self {
        Self {
            data_type,
            name: String::new(),
            is_not_null: None,
            is_primary_key: None,
            foreign_key: None,
            description: None,
        }
    }

    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    /// Set the NOT NULL constraint for this field
    pub fn with_not_null(mut self, is_not_null: bool) -> Self {
        self.is_not_null = Some(is_not_null);
        self
    }

    /// Set the primary key constraint for this field
    pub fn with_primary_key(mut self, is_primary_key: bool) -> Self {
        self.is_primary_key = Some(is_primary_key);
        self
    }

    /// Set the foreign key constraint for this field
    pub fn with_foreign_key(mut self, foreign_key: impl Into<String>) -> Self {
        self.foreign_key = Some(foreign_key.into());
        self
    }

    /// Set the description for this field
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn from_with_table_name_prefix(desc: &ColumnDesc, table_name: &str) -> Self {
        Self {
            data_type: desc.data_type.clone(),
            name: format!("{}.{}", table_name, desc.name),
            is_not_null: None,
            is_primary_key: None,
            foreign_key: None,
            description: desc.description.clone(),
        }
    }
}

impl From<&PbField> for Field {
    fn from(prost_field: &PbField) -> Self {
        Self {
            data_type: DataType::from(prost_field.get_data_type().expect("data type not found")),
            name: prost_field.get_name().clone(),
            is_not_null: prost_field.is_not_null,
            is_primary_key: prost_field.is_primary_key,
            foreign_key: prost_field.foreign_key.clone(),
            description: prost_field.description.clone(),
        }
    }
}

impl Index<usize> for Schema {
    type Output = Field;

    fn index(&self, index: usize) -> &Self::Output {
        &self.fields[index]
    }
}

impl FromIterator<Field> for Schema {
    fn from_iter<I: IntoIterator<Item = Field>>(iter: I) -> Self {
        Schema {
            fields: iter.into_iter().collect::<Vec<_>>(),
            description: None,
        }
    }
}

impl From<&StructType> for Schema {
    fn from(t: &StructType) -> Self {
        Schema::new(
            t.iter()
                .map(|(s, d)| Field::with_name(d.clone(), s))
                .collect(),
        )
    }
}

pub mod test_utils {
    use super::*;

    pub fn field_n<const N: usize>(data_type: DataType) -> Schema {
        Schema::new(vec![Field::unnamed(data_type); N])
    }

    fn int32_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Int32)
    }

    /// Create a util schema **for test only** with two int32 fields.
    pub fn ii() -> Schema {
        int32_n::<2>()
    }

    /// Create a util schema **for test only** with three int32 fields.
    pub fn iii() -> Schema {
        int32_n::<3>()
    }

    fn varchar_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Varchar)
    }

    /// Create a util schema **for test only** with three varchar fields.
    pub fn sss() -> Schema {
        varchar_n::<3>()
    }

    fn decimal_n<const N: usize>() -> Schema {
        field_n::<N>(DataType::Decimal)
    }

    /// Create a util schema **for test only** with three decimal fields.
    pub fn ddd() -> Schema {
        decimal_n::<3>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_with_constraints() {
        // Test creating a field with constraints
        let field = Field::new("id", DataType::Int32)
            .with_not_null(true)
            .with_primary_key(true)
            .with_description("Primary key identifier");

        assert_eq!(field.name, "id");
        assert_eq!(field.is_not_null, Some(true));
        assert_eq!(field.is_primary_key, Some(true));
        assert_eq!(field.description, Some("Primary key identifier".to_string()));
        assert_eq!(field.foreign_key, None);
    }

    #[test]
    fn test_field_with_foreign_key() {
        // Test creating a field with foreign key
        let field = Field::new("user_id", DataType::Int32)
            .with_not_null(true)
            .with_foreign_key("users(id)")
            .with_description("Reference to users table");

        assert_eq!(field.name, "user_id");
        assert_eq!(field.is_not_null, Some(true));
        assert_eq!(field.foreign_key, Some("users(id)".to_string()));
        assert_eq!(field.description, Some("Reference to users table".to_string()));
        assert_eq!(field.is_primary_key, None);
    }

    #[test]
    fn test_schema_with_description() {
        // Test creating a schema with description
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32).with_primary_key(true),
            Field::new("name", DataType::Varchar),
        ])
        .with_description("User information table");

        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.description, Some("User information table".to_string()));
    }

    #[test]
    fn test_field_serialization_roundtrip() {
        // Test that fields with constraints serialize and deserialize correctly
        let original_field = Field::new("test_field", DataType::Varchar)
            .with_not_null(true)
            .with_primary_key(false)
            .with_foreign_key("ref_table(id)")
            .with_description("Test description");

        let pb_field = original_field.to_prost();
        let deserialized_field = Field::from_prost(&pb_field);

        assert_eq!(deserialized_field.name, original_field.name);
        assert_eq!(deserialized_field.data_type, original_field.data_type);
        assert_eq!(deserialized_field.is_not_null, original_field.is_not_null);
        assert_eq!(deserialized_field.is_primary_key, original_field.is_primary_key);
        assert_eq!(deserialized_field.foreign_key, original_field.foreign_key);
        assert_eq!(deserialized_field.description, original_field.description);
    }

    #[test]
    fn test_backward_compatibility() {
        // Test that fields without constraints still work
        let field = Field::new("simple_field", DataType::Int32);

        assert_eq!(field.name, "simple_field");
        assert_eq!(field.is_not_null, None);
        assert_eq!(field.is_primary_key, None);
        assert_eq!(field.foreign_key, None);
        assert_eq!(field.description, None);
    }

    #[test]
    fn test_redshift_table_schema_example() {
        // Example schema for Redshift table with constraints
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

        assert_eq!(schema.fields.len(), 4);
        assert_eq!(schema.description, Some("Orders table with customer references".to_string()));
        
        // Verify primary key field
        assert_eq!(schema.fields[0].is_primary_key, Some(true));
        assert_eq!(schema.fields[0].is_not_null, Some(true));
        
        // Verify foreign key field
        assert_eq!(schema.fields[1].foreign_key, Some("customers(id)".to_string()));
        assert_eq!(schema.fields[1].is_not_null, Some(true));
    }
}
