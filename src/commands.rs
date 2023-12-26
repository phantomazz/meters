use crate::database::ops::Operations;
use crate::database::structs::Meter;

pub type CommandResult<T> = Result<T, String>;
pub type CommandResultNoValue = CommandResult<()>;

pub fn add_meter(ops: &Operations, name: &str) -> CommandResultNoValue {
    let meter_exists = match ops.exists_by_name::<Meter>(name) {
        Ok(exists) => exists,
        Err(error) => return Err(error.to_string()),
    };
    if meter_exists {
        return Err(std::format!("Meter with name {} already exists", name));
    }

    match ops.insert(&Meter::new(name)) {
        Ok(_) => Ok(()),
        Err(error) => Err(std::format!(
            "Couldn't add meter to the database: {}",
            error
        )),
    }
}

#[cfg(test)]
mod test {
    use crate::commands::add_meter;
    use crate::database::create::create_tables_if_do_not_exist;
    use crate::database::ops::Operations;

    #[test]
    fn test_add_meter() {
        let ops = Operations::in_memory().unwrap();

        // no tables yet
        assert!(add_meter(&ops, "meter1").is_err());

        create_tables_if_do_not_exist(ops.get_connection());
        assert!(add_meter(&ops, "meter1").is_ok());
        assert!(add_meter(&ops, "meter2").is_ok());
        // same name again, should fail
        assert!(add_meter(&ops, "meter1").is_err());
    }
}
