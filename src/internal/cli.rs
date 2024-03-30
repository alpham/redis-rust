use clap::Parser;

#[derive(Parser, Debug, Clone, Copy)]
#[command(
    version = "1.0",
    about = "Running redis server that is written in rust"
)]
pub struct CliArgs {
    #[arg(short = 'p', long = "port", default_value = "6379")]
    pub port: i32,
    // #[arg(long = "host", default_value = "127.0.0.1")]
    // pub host: [char],
}
