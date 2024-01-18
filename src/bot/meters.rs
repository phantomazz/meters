use teloxide::{
    prelude::*,
    requests::Requester,
    types::{InlineKeyboardButton, InlineKeyboardMarkup},
};

use super::{HandlerResult, MyDialogue, SharedCommands, State};
use rust_i18n::t;

const ACTION_LIST_METERS: &str = "list_meter";
const ACTION_ADD_METER: &str = "add_meter";
const ACTION_DELETE_METER: &str = "delete_meter";
const ACTION_EDIT_METER: &str = "edit_meter";

pub async fn start_manage_meters(bot: Bot, dialogue: MyDialogue, chat_id: ChatId) -> HandlerResult {
    dialogue.update(State::ManageMeters).await?;
    let keyboard =
        InlineKeyboardMarkup::default().append_row(vec![InlineKeyboardButton::callback(
            t!("button.list-meters"),
            ACTION_LIST_METERS,
        )]);
    bot.send_message(chat_id, t!("message.managing-meters"))
        .reply_markup(keyboard)
        .await?;
    Ok(())
}

pub async fn manage_meters_button(
    bot: Bot,
    my_dialogue: MyDialogue,
    guarded_commands: SharedCommands,
    q: CallbackQuery,
) -> HandlerResult {
    if let Some(msg) = &q.message {
        bot.edit_message_reply_markup(msg.chat.id, msg.id).await?;

        if let Some(data) = &q.data {
            log::debug!("Manage meters button pressed: {}", data);

            if data == ACTION_LIST_METERS {
                list_meters(bot, my_dialogue, guarded_commands, msg.chat.id).await?;
            }
        }
    }
    Ok(())
}

pub async fn list_meters(
    bot: Bot,
    my_dialogue: MyDialogue,
    commands: SharedCommands,
    chat_id: ChatId,
) -> HandlerResult {
    match commands.list_meters().await {
        Ok(found_meters) => {
            bot.send_message(
                chat_id,
                t!(
                    "message.found-meters",
                    count = found_meters.len(),
                    ending = match found_meters.len() {
                        0 => ".".to_string(),
                        _ => std::format!(
                            ": {}.",
                            found_meters
                                .iter()
                                .map(|x| x.name.clone())
                                .collect::<Vec<String>>()
                                .join(", ")
                        ),
                    }
                ),
            )
            .await?;
        }
        Err(error) => {
            bot.send_message(chat_id, std::format!("Couldn't list meters: {}", error))
                .await?;
        }
    };

    start_manage_meters(bot, my_dialogue, chat_id).await?;
    Ok(())
}
