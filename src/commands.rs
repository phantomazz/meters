use crate::database::ops::Operations;
use crate::database::structs::Meter;
use rusqlite::Connection;

pub type CommandResult<T> = Result<T, String>;
pub type CommandResultNoValue = CommandResult<()>;

struct Commands {
    ops: Operations,
}
impl Commands {
    pub fn in_memory() -> CommandResult<Self> {
        match Operations::in_memory() {
            Ok(ops) => Ok(Commands { ops }),
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn get_connection(&self) -> &Connection {
        self.ops.get_connection()
    }

    pub fn add_meter(&self, name: &str) -> CommandResultNoValue {
        let meter_exists = match self.ops.exists_by_name::<Meter>(name) {
            Ok(exists) => exists,
            Err(error) => return Err(error.to_string()),
        };
        if meter_exists {
            return Err(std::format!("Meter with name {} already exists", name));
        }

        match self.ops.insert(&Meter::new(name)) {
            Ok(_) => Ok(()),
            Err(error) => Err(std::format!(
                "Couldn't add meter to the database: {}",
                error
            )),
        }
    }

    pub fn delete_meter(&self, id: u32) -> CommandResultNoValue {
        match self.ops.delete_by_id::<Meter>(id) {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    pub fn list_meters(&self) -> CommandResult<Vec<Meter>> {
        match self.ops.get_all::<Meter>() {
            Ok(rows) => Ok(rows),
            Err(error) => Err(error.to_string()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::commands::Commands;
    use crate::database::create::create_tables_if_do_not_exist;

    #[test]
    fn test_meter_commands() {
        let commands = Commands::in_memory().unwrap();

        // no tables yet
        assert!(commands.add_meter("meter1").is_err());

        create_tables_if_do_not_exist(commands.get_connection());
        assert!(commands.add_meter("meter1").is_ok());
        assert!(commands.add_meter("meter2").is_ok());
        // same name again, should fail
        assert!(commands.add_meter("meter1").is_err());

        let mut meters = commands.list_meters().unwrap();
        assert_eq!(meters.len(), 2);
        assert_eq!(meters[0].name, "meter1");
        assert_eq!(meters[1].name, "meter2");

        assert!(commands.delete_meter(meters[0].id).is_ok());
        assert!(commands.delete_meter(meters[1].id).is_ok());

        meters = commands.list_meters().unwrap();
        assert_eq!(meters.len(), 0);
    }
}
