use super::structs::{Meter, Metric, MetricValue, TableName};
use rusqlite::{self, Connection};

pub trait CreateTable {
    fn create_table(connection: &Connection) -> rusqlite::Result<usize>;
}

impl CreateTable for Meter {
    fn create_table(connection: &Connection) -> rusqlite::Result<usize> {
        connection.execute(
            std::format!(
                "CREATE TABLE {} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                )",
                Meter::TABLE_NAME
            )
            .as_str(),
            (),
        )
    }
}

impl CreateTable for Metric {
    fn create_table(connection: &Connection) -> rusqlite::Result<usize> {
        connection.execute(
            std::format!(
                "CREATE TABLE {} (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    meter_id INTEGER,
                    rate INTEGER,
                    FOREIGN KEY(meter_id) REFERENCES meter(id)
                )",
                Metric::TABLE_NAME
            )
            .as_str(),
            (),
        )
    }
}

impl CreateTable for MetricValue {
    fn create_table(connection: &Connection) -> rusqlite::Result<usize> {
        connection.execute(
            std::format!(
                "CREATE TABLE {} (
                    id INTEGER PRIMARY KEY,
                    metric_id INTEGER,
                    value INTEGER,
                    added STRING,
                    FOREIGN KEY(metric_id) REFERENCES metric(id)
                )",
                MetricValue::TABLE_NAME
            )
            .as_str(),
            (),
        )
    }
}

pub fn table_exists<T: TableName>(connection: &Connection) -> bool {
    let mut statement = connection
        .prepare(&std::format!(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='{}'",
            T::TABLE_NAME
        ))
        .unwrap();
    statement
        .query_map([], |row| row.get::<usize, String>(0))
        .unwrap()
        .map(|x| x.unwrap())
        .collect::<Vec<String>>()
        == vec![T::TABLE_NAME]
}

pub fn create_table_if_does_not_exist<T>(connection: &Connection)
where
    T: TableName,
    T: CreateTable,
{
    if !table_exists::<T>(connection) {
        T::create_table(connection).unwrap();
    }
}

pub fn create_tables_if_do_not_exist(connection: &Connection) {
    create_table_if_does_not_exist::<Meter>(connection);
    create_table_if_does_not_exist::<Metric>(connection);
    create_table_if_does_not_exist::<MetricValue>(connection);
}

#[cfg(test)]
mod test {
    use super::{create_table_if_does_not_exist, create_tables_if_do_not_exist, table_exists};
    use crate::database::create::CreateTable;
    use crate::database::structs::{Meter, Metric, MetricValue};
    use rusqlite::Connection;

    #[test]
    fn test_create_tables() {
        let connection = Connection::open_in_memory().unwrap();

        assert!(!table_exists::<Meter>(&connection));
        assert!(!table_exists::<Metric>(&connection));
        assert!(!table_exists::<MetricValue>(&connection));

        Meter::create_table(&connection).unwrap();
        assert!(table_exists::<Meter>(&connection));

        Metric::create_table(&connection).unwrap();
        assert!(table_exists::<Metric>(&connection));

        MetricValue::create_table(&connection).unwrap();
        assert!(table_exists::<MetricValue>(&connection));
    }

    #[test]
    fn test_create_table_if_does_not_exist() {
        let connection = Connection::open_in_memory().unwrap();

        assert!(!table_exists::<Meter>(&connection));
        create_table_if_does_not_exist::<Meter>(&connection);
        create_table_if_does_not_exist::<Meter>(&connection);
        assert!(table_exists::<Meter>(&connection));
    }

    #[test]
    fn test_create_tables_if_do_not_exist() {
        let connection = Connection::open_in_memory().unwrap();

        assert!(!table_exists::<Meter>(&connection));
        assert!(!table_exists::<Metric>(&connection));
        assert!(!table_exists::<MetricValue>(&connection));

        create_tables_if_do_not_exist(&connection);

        assert!(table_exists::<Meter>(&connection));
        assert!(table_exists::<Metric>(&connection));
        assert!(table_exists::<MetricValue>(&connection));
    }
}
