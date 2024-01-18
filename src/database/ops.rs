use super::query::{Order, Query, WhereExprOperator};
use super::structs::{FieldNames, FromRow, InsertValues, TableName};
use std::marker::{Send, Sync};
use tokio_rusqlite::{Connection, Error};

pub type DatabaseResult<T> = Result<T, Error>;
pub type DatabaseResultNoValue = DatabaseResult<()>;

pub struct Operations {
    connection: Connection,
}

impl Operations {
    pub async fn in_memory() -> DatabaseResult<Self> {
        match Connection::open_in_memory().await {
            Ok(connection) => Ok(Operations { connection }),
            Err(error) => Err(error),
        }
    }

    pub fn get_connection(&self) -> &Connection {
        &self.connection
    }

    pub async fn insert<
        T: Send + TableName + FieldNames + InsertValues + Clone + Send + Sync + 'static,
    >(
        &self,
        entry: T,
    ) -> DatabaseResultNoValue {
        match self
            .connection
            .call(move |connection| {
                Ok(connection
                    .execute(&Query::insert::<T>().to_string(), entry.get_insert_values())?)
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub async fn delete_by_id<T: TableName>(&self, id: u32) -> DatabaseResultNoValue {
        match self
            .connection
            .call(move |connection| {
                Ok(connection.execute(
                    &Query::delete::<T>()
                        .where_("id", WhereExprOperator::Equal, id)
                        .to_string(),
                    (),
                ))
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub async fn exists_by_name<T: TableName + FromRow>(
        &self,
        name: String,
    ) -> DatabaseResult<bool> {
        match self
            .connection
            .call(|connection| {
                let mut statement = connection.prepare(
                    &Query::select::<T>()
                        .where_("name", WhereExprOperator::Equal, name)
                        .to_string(),
                )?;
                let result = !statement
                    .query_map((), |row| Ok(T::from_row(row)))?
                    .map(|x| x.unwrap())
                    .collect::<Vec<T>>()
                    .is_empty();
                Ok(result)
            })
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => Err(error),
        }
    }

    pub async fn get_all<T: TableName + FromRow + Send + Sync + 'static>(
        &self,
    ) -> DatabaseResult<Vec<T>> {
        match self
            .connection
            .call(|connection| {
                let mut statement = connection.prepare(&Query::select::<T>().to_string())?;
                let result = statement
                    .query_map((), |row| Ok(T::from_row(row)))?
                    .map(|x| x.unwrap())
                    .collect();
                Ok(result)
            })
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => Err(error),
        }
    }

    pub async fn get_last<T: TableName + FromRow + Send + Sync + 'static>(
        &self,
    ) -> DatabaseResult<T> {
        match self
            .connection
            .call(|connection| {
                let mut statement = connection.prepare(
                    &Query::select::<T>()
                        .order_by("id", Order::Descending)
                        .limit(1)
                        .to_string(),
                )?;
                let result = statement
                    .query_map((), |row| Ok(T::from_row(row)))?
                    .next()
                    .unwrap()?;
                Ok(result)
            })
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod test {
    use super::Operations;
    use crate::database::{
        create::create_tables_if_do_not_exist,
        structs::{Meter, Metric, MetricValue},
    };
    use chrono::Local;

    #[tokio::test]
    async fn test_insert_and_get_all_records() {
        let ops = Operations::in_memory().await.unwrap();
        create_tables_if_do_not_exist(ops.get_connection()).await;

        ops.insert(Meter::new("meter1")).await.unwrap();
        ops.insert(Meter::new("meter2")).await.unwrap();

        let meters = ops.get_all::<Meter>().await.unwrap();
        assert_eq!(meters.len(), 2);
        assert_eq!(meters[0].name, "meter1");
        assert_eq!(meters[1].name, "meter2");

        ops.insert(Metric::new("metric1", meters[0].id, 100))
            .await
            .unwrap();
        ops.insert(Metric::new("metric2", meters[1].id, 200))
            .await
            .unwrap();

        let metrics = ops.get_all::<Metric>().await.unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "metric1");
        assert_eq!(metrics[0].meter_id, meters[0].id);
        assert_eq!(metrics[0].rate, 100);
        assert_eq!(metrics[1].name, "metric2");
        assert_eq!(metrics[1].meter_id, meters[1].id);
        assert_eq!(metrics[1].rate, 200);

        let now = Local::now().naive_local();
        ops.insert(MetricValue::new(metrics[0].id, 123, &now))
            .await
            .unwrap();
        ops.insert(MetricValue::new(metrics[1].id, 456, &now))
            .await
            .unwrap();

        let values = ops.get_all::<MetricValue>().await.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].metric_id, metrics[0].id);
        assert_eq!(values[0].value, 123);
        assert_eq!(values[0].added, now);
        assert_eq!(values[1].metric_id, metrics[1].id);
        assert_eq!(values[1].value, 456);
        assert_eq!(values[1].added, now);
    }

    #[tokio::test]
    async fn test_insert_and_get_last_records() {
        let ops = Operations::in_memory().await.unwrap();
        create_tables_if_do_not_exist(ops.get_connection()).await;

        ops.insert(Meter::new("meter1")).await.unwrap();
        ops.insert(Meter::new("meter2")).await.unwrap();

        let last_meter = ops.get_last::<Meter>().await.unwrap();
        assert_eq!(last_meter.name, "meter2");

        ops.insert(Metric::new("metric1", last_meter.id, 100))
            .await
            .unwrap();
        ops.insert(Metric::new("metric2", last_meter.id, 200))
            .await
            .unwrap();

        let last_metric = ops.get_last::<Metric>().await.unwrap();
        assert_eq!(last_metric.name, "metric2");
        assert_eq!(last_metric.meter_id, last_meter.id);
        assert_eq!(last_metric.rate, 200);

        let now = Local::now().naive_local();
        ops.insert(MetricValue::new(last_metric.id, 1234, &now))
            .await
            .unwrap();
        ops.insert(MetricValue::new(last_metric.id, 5678, &now))
            .await
            .unwrap();

        let last_metric_value = ops.get_last::<MetricValue>().await.unwrap();
        assert_eq!(last_metric_value.metric_id, last_metric.id);
        assert_eq!(last_metric_value.value, 5678);
        assert_eq!(last_metric_value.added, now);
    }

    #[tokio::test]
    async fn test_exists_by_name() {
        let ops = Operations::in_memory().await.unwrap();
        create_tables_if_do_not_exist(ops.get_connection()).await;

        ops.insert(Meter::new("meter1")).await.unwrap();
        ops.insert(Meter::new("meter2")).await.unwrap();

        assert!(ops
            .exists_by_name::<Meter>("meter1".to_string())
            .await
            .unwrap());
        assert!(ops
            .exists_by_name::<Meter>("meter2".to_string())
            .await
            .unwrap());
        assert!(!ops
            .exists_by_name::<Meter>("meter3".to_string())
            .await
            .unwrap());

        let meters = ops.get_all::<Meter>().await.unwrap();
        ops.insert(Metric::new("metric1", meters[0].id, 100))
            .await
            .unwrap();
        ops.insert(Metric::new("metric2", meters[1].id, 200))
            .await
            .unwrap();

        assert!(ops
            .exists_by_name::<Metric>("metric1".to_string())
            .await
            .unwrap());
        assert!(ops
            .exists_by_name::<Metric>("metric2".to_string())
            .await
            .unwrap());
        assert!(!ops
            .exists_by_name::<Metric>("metric3".to_string())
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn test_delete_by_id() {
        let ops = Operations::in_memory().await.unwrap();
        create_tables_if_do_not_exist(ops.get_connection()).await;

        ops.insert(Meter::new("meter1")).await.unwrap();
        ops.insert(Meter::new("meter2")).await.unwrap();
        let meters = ops.get_all::<Meter>().await.unwrap();

        ops.insert(Metric::new("metric1", meters[1].id, 100))
            .await
            .unwrap();
        ops.insert(Metric::new("metric2", meters[1].id, 200))
            .await
            .unwrap();
        let metrics = ops.get_all::<Metric>().await.unwrap();

        assert!(ops
            .exists_by_name::<Meter>("meter1".to_string())
            .await
            .unwrap());
        assert!(ops
            .exists_by_name::<Meter>("meter2".to_string())
            .await
            .unwrap());
        assert!(ops
            .exists_by_name::<Metric>("metric1".to_string())
            .await
            .unwrap());
        assert!(ops
            .exists_by_name::<Metric>("metric2".to_string())
            .await
            .unwrap());

        assert!(ops.delete_by_id::<Meter>(meters[0].id).await.is_ok());
        assert!(ops.delete_by_id::<Metric>(metrics[1].id).await.is_ok());

        assert!(!ops
            .exists_by_name::<Meter>("meter1".to_string())
            .await
            .unwrap());
        assert!(ops
            .exists_by_name::<Meter>("meter2".to_string())
            .await
            .unwrap());
        assert!(ops
            .exists_by_name::<Metric>("metric1".to_string())
            .await
            .unwrap());
        assert!(!ops
            .exists_by_name::<Metric>("metric2".to_string())
            .await
            .unwrap());
    }
}
