#[allow(dead_code)] // TODO: remove
mod bot;
#[allow(dead_code)] // TODO: remove
mod commands;
#[allow(dead_code)] // TODO: remove
mod database;
#[allow(dead_code)] // TODO: remove
mod lang;

rust_i18n::i18n!("locales");

#[tokio::main]
async fn main() {
    bot::start().await;
}
