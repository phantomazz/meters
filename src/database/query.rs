use std::fmt::Display;

use crate::database::structs::{FieldNames, TableName};

pub struct Query;

pub struct Insert {
    query: String,
}

#[derive(Clone)]
pub enum WhereExprOperator {
    Equal,
    NotEqual,
}

#[derive(Clone)]
struct WhereExpr {
    field: String,
    operator: WhereExprOperator,
    value: String,
}

#[derive(Clone)]
enum WhereElement {
    None(WhereExpr),
    Or(WhereExpr),
    And(WhereExpr),
}

#[derive(Clone)]
struct Where {
    elements: Vec<WhereElement>,
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
    where_info: Option<Where>,
}

pub struct WhereActions {
    select: Select,
}

impl WhereActions {
    pub fn start_where<T: Display>(
        select: &Select,
        field: &str,
        operator: WhereExprOperator,
        value: T,
    ) -> Self {
        WhereActions {
            select: Select {
                where_info: Some(Where {
                    elements: vec![WhereElement::None(WhereExpr {
                        field: field.to_string(),
                        operator,
                        value: value.to_string(),
                    })],
                }),
                ..select.clone()
            },
        }
    }

    pub fn stop_where(&self) -> Select {
        self.select.clone()
    }

    fn new_conjunction(&self, element: WhereElement) -> Self {
        let mut new_elements = match &self.select.where_info {
            Some(where_info) => where_info.elements.clone(),
            None => panic!("where_info cannot be empty at this point"),
        };
        new_elements.push(element);

        WhereActions {
            select: Select {
                where_info: Some(Where {
                    elements: new_elements,
                }),
                ..self.select.clone()
            },
        }
    }

    pub fn or<T: Display>(&self, field: &str, operator: WhereExprOperator, value: T) -> Self {
        self.new_conjunction(WhereElement::Or(WhereExpr {
            field: field.to_string(),
            operator,
            value: value.to_string(),
        }))
    }

    pub fn and<T: Display>(&self, field: &str, operator: WhereExprOperator, value: T) -> Self {
        self.new_conjunction(WhereElement::And(WhereExpr {
            field: field.to_string(),
            operator,
            value: value.to_string(),
        }))
    }
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
            where_info: None,
        }
    }

    pub fn where_<T: Display>(
        &self,
        field: &str,
        operator: WhereExprOperator,
        value: T,
    ) -> WhereActions {
        WhereActions::start_where(&self.clone(), field, operator, value)
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

impl Display for WhereExprOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match *self {
                WhereExprOperator::Equal => "=",
                WhereExprOperator::NotEqual => "!=",
            }
        )
    }
}

impl Display for WhereExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.field,
            self.operator,
            match &self.value.chars().all(|x| x.is_numeric()) {
                true => self.value.to_string(),
                false => std::format!("'{}'", self.value),
            }
        )
    }
}

impl Display for WhereElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WhereElement::None(expr) => write!(f, "{}", expr),
            WhereElement::Or(expr) => write!(f, "OR {}", expr),
            WhereElement::And(expr) => write!(f, "AND {}", expr),
        }
    }
}

impl Display for Where {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            " WHERE {}",
            self.elements
                .iter()
                .map(|x| x.to_string())
                .collect::<Vec<String>>()
                .join(" ")
        )
    }
}

impl Display for WhereActions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.select)
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
            "{}{}{}{}",
            self.query,
            option_to_string(&self.where_info),
            option_to_string(&self.order_info),
            option_to_string(&self.limit_info)
        )
    }
}

#[cfg(test)]
mod test {
    use crate::database::{
        query::{Order, Query, WhereExprOperator},
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
    fn test_select_order_and_limit() {
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

    #[test]
    fn test_select_where() {
        assert_eq!(
            Query::select::<Meter>()
                .where_("name", WhereExprOperator::Equal, "meter1")
                .to_string(),
            "SELECT * FROM meter WHERE name = 'meter1'"
        );
        assert_eq!(
            Query::select::<Meter>()
                .where_("id", WhereExprOperator::Equal, 123)
                .to_string(),
            "SELECT * FROM meter WHERE id = 123"
        );
        assert_eq!(
            Query::select::<Meter>()
                .where_("id", WhereExprOperator::Equal, 123)
                .stop_where()
                .to_string(),
            "SELECT * FROM meter WHERE id = 123"
        );
        assert_eq!(
            Query::select::<Meter>()
                .where_("id", WhereExprOperator::Equal, 123)
                .or("name", WhereExprOperator::NotEqual, "some_name")
                .to_string(),
            "SELECT * FROM meter WHERE id = 123 OR name != 'some_name'"
        );
        assert_eq!(
            Query::select::<Meter>()
                .where_("id", WhereExprOperator::Equal, 123)
                .and("name", WhereExprOperator::NotEqual, "some_name")
                .to_string(),
            "SELECT * FROM meter WHERE id = 123 AND name != 'some_name'"
        );
        assert_eq!(
            Query::select::<Meter>()
                .where_("id", WhereExprOperator::Equal, 123)
                .and("name", WhereExprOperator::NotEqual, "some_name")
                .or("surname", WhereExprOperator::Equal, "some_surname")
                .to_string(),
            "SELECT * FROM meter WHERE id = 123 AND name != 'some_name' OR surname = 'some_surname'"
        );
    }

    #[test]
    fn test_select_complex() {
        assert_eq!(
            Query::select::<MetricValue>()
                .limit(456)
                .order_by("id", Order::Descending)
                .where_("id", WhereExprOperator::Equal, 123)
                .and("name", WhereExprOperator::NotEqual, "some_name")
                .or("surname", WhereExprOperator::Equal, "some_surname")
                .to_string(),
            "SELECT * FROM metric_value WHERE id = 123 AND name != 'some_name' OR surname = 'some_surname' ORDER BY id DESC LIMIT 456"
        );
        assert_eq!(
            Query::select::<MetricValue>()
                .where_("id", WhereExprOperator::Equal, 123)
                .and("name", WhereExprOperator::NotEqual, "some_name")
                .or("surname", WhereExprOperator::Equal, "some_surname")
                .stop_where()
                .limit(456)
                .order_by("id", Order::Descending)
                .to_string(),
            "SELECT * FROM metric_value WHERE id = 123 AND name != 'some_name' OR surname = 'some_surname' ORDER BY id DESC LIMIT 456"
        );
    }
}
