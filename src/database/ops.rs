use super::query::{Order, Query, WhereExprOperator};
use super::structs::{FieldNames, FromRow, InsertValues, TableName};
use rusqlite::{Connection, Error};

pub type DatabaseResult<T> = Result<T, Error>;
pub type DatabaseResultNoValue = DatabaseResult<()>;

pub struct Operations {
    connection: Connection,
}

impl Operations {
    pub fn in_memory() -> DatabaseResult<Self> {
        match Connection::open_in_memory() {
            Ok(connection) => Ok(Operations { connection }),
            Err(error) => Err(error),
        }
    }

    pub fn get_connection(&self) -> &Connection {
        &self.connection
    }

    pub fn insert<T: TableName + FieldNames + InsertValues>(
        &self,
        entry: &T,
    ) -> DatabaseResultNoValue {
        match self
            .connection
            .execute(&Query::insert::<T>().to_string(), entry.get_insert_values())
        {
            Ok(_) => Ok(()),
            Err(error) => Err(error),
        }
    }

    pub fn exists_by_name<T: TableName + FromRow>(&self, name: &str) -> DatabaseResult<bool> {
        let mut statement = self.connection.prepare(
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
    }

    pub fn get_all<T: TableName + FromRow>(&self) -> DatabaseResult<Vec<T>> {
        let mut statement = self.connection.prepare(&Query::select::<T>().to_string())?;
        let result = statement
            .query_map((), |row| Ok(T::from_row(row)))?
            .map(|x| x.unwrap())
            .collect();
        Ok(result)
    }

    pub fn get_last<T: TableName + FromRow>(&self) -> DatabaseResult<T> {
        let mut statement = self.connection.prepare(
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

    #[test]
    fn test_insert_and_get_all_records() {
        let ops = Operations::in_memory().unwrap();
        create_tables_if_do_not_exist(ops.get_connection());

        ops.insert(&Meter::new("meter1")).unwrap();
        ops.insert(&Meter::new("meter2")).unwrap();

        let meters = ops.get_all::<Meter>().unwrap();
        assert_eq!(meters.len(), 2);
        assert_eq!(meters[0].name, "meter1");
        assert_eq!(meters[1].name, "meter2");

        ops.insert(&Metric::new("metric1", meters[0].id, 100))
            .unwrap();
        ops.insert(&Metric::new("metric2", meters[1].id, 200))
            .unwrap();

        let metrics = ops.get_all::<Metric>().unwrap();
        assert_eq!(metrics.len(), 2);
        assert_eq!(metrics[0].name, "metric1");
        assert_eq!(metrics[0].meter_id, meters[0].id);
        assert_eq!(metrics[0].rate, 100);
        assert_eq!(metrics[1].name, "metric2");
        assert_eq!(metrics[1].meter_id, meters[1].id);
        assert_eq!(metrics[1].rate, 200);

        let now = Local::now().naive_local();
        ops.insert(&MetricValue::new(metrics[0].id, 123, &now))
            .unwrap();
        ops.insert(&MetricValue::new(metrics[1].id, 456, &now))
            .unwrap();

        let values = ops.get_all::<MetricValue>().unwrap();
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
        let ops = Operations::in_memory().unwrap();
        create_tables_if_do_not_exist(ops.get_connection());

        ops.insert(&Meter::new("meter1")).unwrap();
        ops.insert(&Meter::new("meter2")).unwrap();

        let last_meter = ops.get_last::<Meter>().unwrap();
        assert_eq!(last_meter.name, "meter2");

        ops.insert(&Metric::new("metric1", last_meter.id, 100))
            .unwrap();
        ops.insert(&Metric::new("metric2", last_meter.id, 200))
            .unwrap();

        let last_metric = ops.get_last::<Metric>().unwrap();
        assert_eq!(last_metric.name, "metric2");
        assert_eq!(last_metric.meter_id, last_meter.id);
        assert_eq!(last_metric.rate, 200);

        let now = Local::now().naive_local();
        ops.insert(&MetricValue::new(last_metric.id, 1234, &now))
            .unwrap();
        ops.insert(&MetricValue::new(last_metric.id, 5678, &now))
            .unwrap();

        let last_metric_value = ops.get_last::<MetricValue>().unwrap();
        assert_eq!(last_metric_value.metric_id, last_metric.id);
        assert_eq!(last_metric_value.value, 5678);
        assert_eq!(last_metric_value.added, now);
    }
    #[test]
    fn test_exists_by_name() {
        let ops = Operations::in_memory().unwrap();
        create_tables_if_do_not_exist(ops.get_connection());

        ops.insert(&Meter::new("meter1")).unwrap();
        ops.insert(&Meter::new("meter2")).unwrap();

        assert!(ops.exists_by_name::<Meter>("meter1").unwrap());
        assert!(ops.exists_by_name::<Meter>("meter2").unwrap());
        assert!(!ops.exists_by_name::<Meter>("meter3").unwrap());

        let meters = ops.get_all::<Meter>().unwrap();
        ops.insert(&Metric::new("metric1", meters[0].id, 100))
            .unwrap();
        ops.insert(&Metric::new("metric2", meters[1].id, 200))
            .unwrap();

        assert!(ops.exists_by_name::<Metric>("metric1").unwrap());
        assert!(ops.exists_by_name::<Metric>("metric2").unwrap());
        assert!(!ops.exists_by_name::<Metric>("metric3").unwrap());
    }
}
