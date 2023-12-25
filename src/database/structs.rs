use chrono::NaiveDateTime;
use meters_derive::{FieldNames, FromRow, InsertValues, TableName};
use rusqlite::{Params, Row};

const NON_EXISTENT_INDEX: u32 = 0;

pub trait Named {}

pub trait TableName {
    const TABLE_NAME: &'static str;
}

pub trait FieldNames {
    fn get_field_names() -> Vec<&'static str>;
}

pub trait InsertValues {
    type Values: Params;

    fn get_insert_values(&self) -> Self::Values;
}

pub trait FromRow {
    fn from_row(row: &Row) -> Self;
}

#[derive(Debug, TableName, FieldNames, InsertValues, FromRow)]
pub struct Meter {
    pub id: u32,
    pub name: String,
}

#[derive(Debug, TableName, FieldNames, InsertValues, FromRow)]
pub struct Metric {
    pub id: u32,
    pub name: String,
    pub meter_id: u32,
    pub rate: u32,
}

#[derive(Debug, TableName, FieldNames, InsertValues, FromRow)]
pub struct MetricValue {
    pub id: u32,
    pub metric_id: u32,
    pub value: u32,
    pub added: NaiveDateTime,
}

impl Named for Meter {}

impl Meter {
    pub fn new(name: &str) -> Self {
        Meter {
            id: NON_EXISTENT_INDEX,
            name: name.to_string(),
        }
    }
}

impl Named for Metric {}

impl Metric {
    pub fn new(name: &str, meter_id: u32, rate: u32) -> Self {
        Metric {
            id: NON_EXISTENT_INDEX,
            name: name.to_string(),
            meter_id,
            rate,
        }
    }
}

impl MetricValue {
    pub fn new(metric_id: u32, value: u32, added: &NaiveDateTime) -> Self {
        MetricValue {
            id: NON_EXISTENT_INDEX,
            metric_id,
            value,
            added: *added,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::database::structs::{
        FieldNames, InsertValues, Meter, Metric, MetricValue, TableName,
    };
    use chrono::Local;

    #[test]
    fn test_table_names() {
        assert_eq!(Meter::TABLE_NAME, "meter");
        assert_eq!(Metric::TABLE_NAME, "metric");
        assert_eq!(MetricValue::TABLE_NAME, "metric_value");
    }

    #[test]
    fn test_field_names() {
        assert_eq!(Meter::get_field_names(), vec!["id", "name"]);
        assert_eq!(
            Metric::get_field_names(),
            vec!["id", "name", "meter_id", "rate"]
        );
        assert_eq!(
            MetricValue::get_field_names(),
            vec!["id", "metric_id", "value", "added"]
        );
    }

    #[test]
    fn test_insert_values() {
        assert_eq!(
            Meter::new("meter1").get_insert_values(),
            ("meter1".to_string(),)
        );
        assert_eq!(
            Metric::new("metric1", 123, 456).get_insert_values(),
            ("metric1".to_string(), 123, 456)
        );

        let now = Local::now().naive_local();
        assert_eq!(
            MetricValue::new(123, 456, &now).get_insert_values(),
            (123, 456, now)
        );
    }
}
