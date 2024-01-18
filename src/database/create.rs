use super::structs::{Meter, Metric, MetricValue, TableName};
use tokio_rusqlite::Connection;

pub trait CreateTable {
    async fn create_table(connection: &Connection) -> tokio_rusqlite::Result<usize>;
}

impl CreateTable for Meter {
    async fn create_table(connection: &Connection) -> tokio_rusqlite::Result<usize> {
        connection
            .call(|connection| {
                Ok(connection.execute(
                    std::format!(
                        "CREATE TABLE {} (
                                id INTEGER PRIMARY KEY,
                                name TEXT NOT NULL
                        )",
                        Meter::TABLE_NAME
                    )
                    .as_str(),
                    (),
                )?)
            })
            .await
    }
}

impl CreateTable for Metric {
    async fn create_table(connection: &Connection) -> tokio_rusqlite::Result<usize> {
        connection
            .call(|connection| {
                Ok(connection.execute(
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
                )?)
            })
            .await
    }
}

impl CreateTable for MetricValue {
    async fn create_table(connection: &Connection) -> tokio_rusqlite::Result<usize> {
        connection
            .call(|connection| {
                Ok(connection.execute(
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
                )?)
            })
            .await
    }
}

pub async fn table_exists<T: TableName>(connection: &Connection) -> bool {
    let check_result = connection
        .call(|connection| {
            let mut statement = connection
                .prepare(&std::format!(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='{}'",
                    T::TABLE_NAME
                ))
                .unwrap();
            Ok(statement
                .query_map([], |row| row.get::<usize, String>(0))
                .unwrap()
                .map(|x| x.unwrap())
                .collect::<Vec<String>>()
                == vec![T::TABLE_NAME])
        })
        .await;
    check_result.unwrap_or(false)
}

pub async fn create_table_if_does_not_exist<T>(connection: &Connection)
where
    T: TableName,
    T: CreateTable,
{
    if !table_exists::<T>(connection).await {
        T::create_table(connection).await.unwrap();
    }
}

pub async fn create_tables_if_do_not_exist(connection: &Connection) {
    create_table_if_does_not_exist::<Meter>(connection).await;
    create_table_if_does_not_exist::<Metric>(connection).await;
    create_table_if_does_not_exist::<MetricValue>(connection).await;
}

#[cfg(test)]
mod test {
    use super::{create_table_if_does_not_exist, create_tables_if_do_not_exist, table_exists};
    use crate::database::create::CreateTable;
    use crate::database::structs::{Meter, Metric, MetricValue};
    use tokio_rusqlite::Connection;

    #[tokio::test]
    async fn test_create_tables() {
        let connection = Connection::open_in_memory().await.unwrap();

        assert!(!table_exists::<Meter>(&connection).await);
        assert!(!table_exists::<Metric>(&connection).await);
        assert!(!table_exists::<MetricValue>(&connection).await);

        Meter::create_table(&connection).await.unwrap();
        assert!(table_exists::<Meter>(&connection).await);

        Metric::create_table(&connection).await.unwrap();
        assert!(table_exists::<Metric>(&connection).await);

        MetricValue::create_table(&connection).await.unwrap();
        assert!(table_exists::<MetricValue>(&connection).await);
    }

    #[tokio::test]
    async fn test_create_table_if_does_not_exist() {
        let connection = Connection::open_in_memory().await.unwrap();

        assert!(!table_exists::<Meter>(&connection).await);
        create_table_if_does_not_exist::<Meter>(&connection).await;
        create_table_if_does_not_exist::<Meter>(&connection).await;
        assert!(table_exists::<Meter>(&connection).await);
    }

    #[tokio::test]
    async fn test_create_tables_if_do_not_exist() {
        let connection = Connection::open_in_memory().await.unwrap();

        assert!(!table_exists::<Meter>(&connection).await);
        assert!(!table_exists::<Metric>(&connection).await);
        assert!(!table_exists::<MetricValue>(&connection).await);

        create_tables_if_do_not_exist(&connection).await;

        assert!(table_exists::<Meter>(&connection).await);
        assert!(table_exists::<Metric>(&connection).await);
        assert!(table_exists::<MetricValue>(&connection).await);
    }
}
