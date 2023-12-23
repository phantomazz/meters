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

pub fn get_all<T: TableName + FromRow>(connection: &Connection) -> Vec<T> {
    let mut statement = connection
        .prepare(&std::format!("SELECT * FROM {};", T::TABLE_NAME))
        .unwrap();
    statement
        .query_map((), |row| Ok(T::from_row(row)))
        .unwrap()
        .map(|x| x.unwrap())
        .collect()
}

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
    use super::{get_all, get_last, insert};
    use crate::database::{
        create::create_tables_if_do_not_exist,
        structs::{Meter, Metric, MetricValue},
    };
    use chrono::Local;
    use rusqlite::Connection;

    #[test]
    fn test_insert_and_get_all_records() {
        let connection = Connection::open_in_memory().unwrap();
        create_tables_if_do_not_exist(&connection);

        insert(&connection, &Meter::new("meter1"));
        insert(&connection, &Meter::new("meter2"));

        let meters = get_all::<Meter>(&connection);
        assert_eq!(meters.len(), 2);
        assert_eq!(meters[0].name, "meter1");
        assert_eq!(meters[1].name, "meter2");

        insert(&connection, &Metric::new("metric1", meters[0].id, 100));
        insert(&connection, &Metric::new("metric2", meters[1].id, 200));

        let metrics = get_all::<Metric>(&connection);
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "metric1");
        assert_eq!(metrics[0].meter_id, meters[0].id);
        assert_eq!(metrics[0].rate, 100);
        assert_eq!(metrics[1].name, "metric2");
        assert_eq!(metrics[1].meter_id, meters[1].id);
        assert_eq!(metrics[1].rate, 200);

        let now = Local::now().naive_local();
        insert(&connection, &MetricValue::new(metrics[0].id, 123, &now));
        insert(&connection, &MetricValue::new(metrics[1].id, 456, &now));

        let values = get_all::<MetricValue>(&connection);
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].metric_id, metrics[0].id);
        assert_eq!(values[0].value, 123);
        assert_eq!(values[0].added, now);
        assert_eq!(values[1].metric_id, metrics[1].id);
        assert_eq!(values[1].value, 456);
        assert_eq!(values[1].added, now);
    }

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
