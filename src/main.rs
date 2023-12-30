#[allow(dead_code)] // TODO: remove
mod bot;
#[allow(dead_code)] // TODO: remove
mod commands;
#[allow(dead_code)] // TODO: remove
mod database;

#[tokio::main]
async fn main() {
    bot::start().await;
}
