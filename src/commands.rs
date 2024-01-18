use crate::database::ops::Operations;
use crate::database::structs::Meter;
use tokio_rusqlite::Connection;

pub type CommandResult<T> = Result<T, String>;
pub type CommandResultNoValue = CommandResult<()>;

pub struct Commands {
    ops: Operations,
}
impl Commands {
    pub async fn in_memory() -> CommandResult<Self> {
        match Operations::in_memory().await {
            Ok(ops) => Ok(Commands { ops }),
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn get_connection(&self) -> &Connection {
        self.ops.get_connection()
    }

    pub async fn add_meter(&self, name: &str) -> CommandResultNoValue {
        let meter_exists = match self.ops.exists_by_name::<Meter>(name.to_string()).await {
            Ok(exists) => exists,
            Err(error) => return Err(error.to_string()),
        };
        if meter_exists {
            return Err(std::format!("Meter with name {} already exists", name));
        }

        match self.ops.insert(Meter::new(name)).await {
            Ok(_) => Ok(()),
            Err(error) => Err(std::format!(
                "Couldn't add meter to the database: {}",
                error
            )),
        }
    }

    pub async fn delete_meter(&self, id: u32) -> CommandResultNoValue {
        match self.ops.delete_by_id::<Meter>(id).await {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    pub async fn list_meters(&self) -> CommandResult<Vec<Meter>> {
        match self.ops.get_all::<Meter>().await {
            Ok(rows) => Ok(rows),
            Err(error) => Err(error.to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::commands::Commands;
    use crate::database::create::create_tables_if_do_not_exist;

    #[tokio::test]
    async fn test_meter_commands() {
        let commands = Commands::in_memory().await.unwrap();

        // no tables yet
        assert!(commands.add_meter("meter1").await.is_err());

        create_tables_if_do_not_exist(commands.get_connection()).await;
        assert!(commands.add_meter("meter1").await.is_ok());
        assert!(commands.add_meter("meter2").await.is_ok());
        // same name again, should fail
        assert!(commands.add_meter("meter1").await.is_err());

        let mut meters = commands.list_meters().await.unwrap();
        assert_eq!(meters.len(), 2);
        assert_eq!(meters[0].name, "meter1");
        assert_eq!(meters[1].name, "meter2");

        assert!(commands.delete_meter(meters[0].id).await.is_ok());
        assert!(commands.delete_meter(meters[1].id).await.is_ok());

        meters = commands.list_meters().await.unwrap();
        assert_eq!(meters.len(), 0);
    }
}
