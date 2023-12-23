use super::structs::{FieldNames, FromRow, InsertValues, TableName};
use rusqlite::Connection;

pub fn insert<T: TableName + FieldNames + InsertValues>(connection: &Connection, entry: &T) {
    let fields = T::get_field_names()
        .into_iter()
        .skip(1)
        .collect::<Vec<&str>>()
        .join(",");

    let placeholders = (0..T::get_field_names().len() - 1)
        .map(|x| std::format!("?{}", x + 1))
        .collect::<Vec<String>>()
        .join(",");

    connection
        .execute(
            std::format!(
                "INSERT INTO {} ({}) VALUES ({})",
                T::TABLE_NAME,
                fields,
                placeholders
            )
            .as_str(),
            entry.get_insert_values(),
        )
        .unwrap();
}

// TODO: get_all

pub fn get_last<T: TableName + FromRow>(connection: &Connection) -> T {
    let mut statement = connection
        .prepare(&std::format!(
            "SELECT * FROM {} ORDER BY id DESC LIMIT 1;",
            T::TABLE_NAME
        ))
        .unwrap();
    let result = statement
        .query_map((), |row| Ok(T::from_row(row)))
        .unwrap()
        .next()
        .unwrap()
        .unwrap();
    result
}

#[cfg(test)]
mod test {
    use super::{get_last, insert};
    use crate::database::{
        create::create_tables_if_do_not_exist,
        structs::{Meter, Metric, MetricValue},
    };
    use chrono::Local;
    use rusqlite::Connection;

    #[test]
    fn test_insert_and_get_last_records() {
        let connection = Connection::open_in_memory().unwrap();
        create_tables_if_do_not_exist(&connection);

        insert(&connection, &Meter::new("meter1"));
        insert(&connection, &Meter::new("meter2"));

        let last_meter = get_last::<Meter>(&connection);
        assert_eq!(last_meter.name, "meter2");

        insert(&connection, &Metric::new("metric1", last_meter.id, 100));
        insert(&connection, &Metric::new("metric2", last_meter.id, 200));

        let last_metric = get_last::<Metric>(&connection);
        assert_eq!(last_metric.name, "metric2");
        assert_eq!(last_metric.meter_id, last_meter.id);
        assert_eq!(last_metric.rate, 200);

        let now = Local::now().naive_local();
        insert(&connection, &MetricValue::new(last_metric.id, 1234, &now));
        insert(&connection, &MetricValue::new(last_metric.id, 5678, &now));

        let last_metric_value = get_last::<MetricValue>(&connection);
        assert_eq!(last_metric_value.metric_id, last_metric.id);
        assert_eq!(last_metric_value.value, 5678);
        assert_eq!(last_metric_value.added, now);
    }
}
