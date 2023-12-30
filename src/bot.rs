mod meters;

use teloxide::{
    dispatching::dialogue::{self, InMemStorage},
    handler,
    prelude::*,
    types::{InlineKeyboardButton, InlineKeyboardMarkup},
    utils::command::BotCommands,
};

use crate::commands;
use std::sync::{Arc, Mutex};

const ALLOWED_CHAT_ID1: ChatId = ChatId(67647522);
const ALLOWED_CHAT_ID2: ChatId = ChatId(62416549);

pub type SharedCommands = Arc<Mutex<commands::Commands>>;

fn is_allowed_chat(chat_id: ChatId) -> bool {
    chat_id == ALLOWED_CHAT_ID1
}

type MyDialogue = Dialogue<State, InMemStorage<State>>;
type HandlerResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Default)]
pub enum State {
    #[default]
    Start,
    State1,
    State2 {
        user_input: String,
    },
}

#[derive(BotCommands, Clone)]
#[command(
    rename_rule = "lowercase",
    description = "These commands are supported:"
)]
enum Command {
    #[command(description = "start the purchase procedure.")]
    Start,
}

pub async fn start() {
    pretty_env_logger::init();
    log::info!("Starting the bot...");

    let commands = Arc::new(Mutex::new(commands::Commands::in_memory().unwrap()));
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
                    .branch(handler![State::State1].endpoint(state1_button)),
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
    _dialogue: MyDialogue,
    _commands: SharedCommands,
    msg: Message,
) -> HandlerResult {
    let keyboard = InlineKeyboardMarkup::default().append_row(vec![
        InlineKeyboardButton::callback("state1", "state1_data"),
        InlineKeyboardButton::callback("state2", "state2_data"),
    ]);

    bot.send_message(msg.chat.id, "Let's start! What would you like to do?")
        .reply_markup(keyboard)
        .await?;
    Ok(())
}

async fn start_button(bot: Bot, my_dialogue: MyDialogue, q: CallbackQuery) -> HandlerResult {
    if let Some(msg) = &q.message {
        bot.edit_message_reply_markup(msg.chat.id, msg.id).await?;

        if let Some(data) = &q.data {
            log::debug!("Start button pressed: {}", data);
            if data == "state1_data" {
                let keyboard = InlineKeyboardMarkup::default()
                    .append_row(vec![InlineKeyboardButton::callback("Cancel", "cancel")]);
                bot.send_message(msg.chat.id, "We're now in state1")
                    .reply_markup(keyboard)
                    .await?;
                my_dialogue.update(State::State1).await?;
            }
        }
    }

    Ok(())
}

async fn state1_button(_bot: Bot, my_dialogue: MyDialogue, _q: CallbackQuery) -> HandlerResult {
    log::debug!("State1 button pressed");

    my_dialogue.update(State::Start).await?;
    Ok(())
}
