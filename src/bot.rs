mod meters;

use teloxide::{
    dispatching::dialogue::{self, InMemStorage},
    handler,
    prelude::*,
    types::{InlineKeyboardButton, InlineKeyboardMarkup},
    utils::command::BotCommands,
};

use crate::{bot::meters::manage_meters_button, database::create::create_tables_if_do_not_exist};
use crate::{bot::meters::start_manage_meters, commands};
use std::sync::Arc;

const ALLOWED_CHAT_ID1: ChatId = ChatId(67647522);
const ALLOWED_CHAT_ID2: ChatId = ChatId(62416549);

const ACTION_MANAGE_METERS: &str = "manage_meters";

pub type SharedCommands = Arc<commands::Commands>;

fn is_allowed_chat(chat_id: ChatId) -> bool {
    chat_id == ALLOWED_CHAT_ID1
}

type MyDialogue = Dialogue<State, InMemStorage<State>>;
type HandlerResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Default)]
pub enum State {
    #[default]
    Start,
    ManageMeters,
}

#[derive(BotCommands, Clone)]
#[command(
    rename_rule = "lowercase",
    description = "These commands are supported:"
)]
enum Command {
    #[command(description = "Start working with the system")]
    Start,
}

pub async fn start() {
    pretty_env_logger::init();
    log::info!("Starting the bot...");

    let commands = Arc::new(commands::Commands::in_memory().await.unwrap());
    create_tables_if_do_not_exist(commands.get_connection()).await;

    let bot = Bot::from_env();

    Dispatcher::builder(
        bot,
        dialogue::enter::<Update, InMemStorage<State>, State, _>()
            .branch(
                Update::filter_message().branch(
                    teloxide::filter_command::<Command, _>()
                        .branch(handler![Command::Start].endpoint(start_command)),
                ),
            )
            .branch(
                Update::filter_callback_query()
                    .branch(handler![State::Start].endpoint(start_button))
                    .branch(handler![State::ManageMeters].endpoint(manage_meters_button)),
            ),
    )
    .dependencies(dptree::deps![InMemStorage::<State>::new(), commands])
    .enable_ctrlc_handler()
    .build()
    .dispatch()
    .await;
}

async fn start_command(
    bot: Bot,
    dialogue: MyDialogue,
    _commands: SharedCommands,
    msg: Message,
) -> HandlerResult {
    dialogue.update(State::Start).await?;
    let keyboard =
        InlineKeyboardMarkup::default().append_row(vec![InlineKeyboardButton::callback(
            "Manage meters",
            ACTION_MANAGE_METERS,
        )]);

    bot.send_message(msg.chat.id, "Let's start! What would you like to do?")
        .reply_markup(keyboard)
        .await?;
    Ok(())
}

async fn start_button(
    bot: Bot,
    my_dialogue: MyDialogue,
    _commands: SharedCommands,
    q: CallbackQuery,
) -> HandlerResult {
    if let Some(msg) = &q.message {
        bot.edit_message_reply_markup(msg.chat.id, msg.id).await?;

        if let Some(data) = &q.data {
            log::debug!("Start button pressed: {}", data);
            if data == ACTION_MANAGE_METERS {
                start_manage_meters(bot, my_dialogue, msg.chat.id).await?;
            }
        }
    }
    Ok(())
}
