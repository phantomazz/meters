use std::fmt::Display;

use crate::database::structs::{FieldNames, TableName};

pub struct Query {
    query: String,
}

pub struct Insert {
    query: String,
}

#[derive(Clone)]
pub enum Order {
    Ascending,
    Descending,
}

#[derive(Clone)]
struct OrderBy {
    field: String,
    order: Order,
}

#[derive(Clone)]
struct Limit {
    limit_to: usize,
}

#[derive(Clone)]
pub struct Select {
    query: String,
    order_info: Option<OrderBy>,
    limit_info: Option<Limit>,
}

impl Insert {
    fn new<T: TableName + FieldNames>() -> Self {
        let fields = T::get_field_names()
            .into_iter()
            .skip(1)
            .collect::<Vec<&str>>()
            .join(",");

        let placeholders = (0..T::get_field_names().len() - 1)
            .map(|x| std::format!("?{}", x + 1))
            .collect::<Vec<String>>()
            .join(",");

        Insert {
            query: std::format!(
                "INSERT INTO {} ({}) VALUES ({})",
                T::TABLE_NAME,
                fields,
                placeholders
            ),
        }
    }
}

impl Select {
    fn new<T: TableName>() -> Self {
        Select {
            query: std::format!("SELECT * FROM {}", T::TABLE_NAME),
            order_info: None,
            limit_info: None,
        }
    }

    pub fn limit(&self, limit_to: usize) -> Self {
        Select {
            limit_info: Some(Limit { limit_to }),
            ..self.clone()
        }
    }

    pub fn order_by(&self, field: &str, order: Order) -> Self {
        Select {
            order_info: Some(OrderBy {
                field: field.to_string(),
                order,
            }),
            ..self.clone()
        }
    }
}

impl Query {
    pub fn insert<T: TableName + FieldNames>() -> Insert {
        Insert::new::<T>()
    }

    pub fn select<T: TableName>() -> Select {
        Select::new::<T>()
    }
}

fn option_to_string<T: Display>(value: &Option<T>) -> String {
    match value {
        Some(some_value) => some_value.to_string(),
        None => "".to_string(),
    }
}

impl Display for OrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            " ORDER BY {}{}",
            self.field,
            match self.order {
                Order::Ascending => "",
                Order::Descending => " DESC",
            }
        )
    }
}

impl Display for Limit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, " LIMIT {}", self.limit_to)
    }
}

impl Display for Insert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.query)
    }
}

impl Display for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.query,
            option_to_string(&self.order_info),
            option_to_string(&self.limit_info)
        )
    }
}

#[cfg(test)]
mod test {
    use crate::database::{
        query::{Order, Query},
        structs::{Meter, Metric, MetricValue},
    };
    #[test]
    fn test_insert() {
        assert_eq!(
            Query::insert::<Meter>().to_string(),
            "INSERT INTO meter (name) VALUES (?1)"
        );
    }

    #[test]
    fn test_select() {
        assert_eq!(Query::select::<Meter>().to_string(), "SELECT * FROM meter");
        assert_eq!(
            Query::select::<Meter>()
                .order_by("id", Order::Ascending)
                .to_string(),
            "SELECT * FROM meter ORDER BY id"
        );
        assert_eq!(
            Query::select::<Metric>()
                .order_by("id", Order::Descending)
                .to_string(),
            "SELECT * FROM metric ORDER BY id DESC"
        );
        assert_eq!(
            Query::select::<Meter>().limit(123).to_string(),
            "SELECT * FROM meter LIMIT 123"
        );
        assert_eq!(
            Query::select::<Meter>()
                .order_by("id", Order::Descending)
                .limit(456)
                .to_string(),
            "SELECT * FROM meter ORDER BY id DESC LIMIT 456"
        );
        assert_eq!(
            Query::select::<MetricValue>()
                .limit(456)
                .order_by("id", Order::Descending)
                .to_string(),
            "SELECT * FROM metric_value ORDER BY id DESC LIMIT 456"
        );
    }
}
