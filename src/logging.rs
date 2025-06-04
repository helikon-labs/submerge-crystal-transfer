use env_logger::{Builder, Env, Target, WriteStyle};
use log::LevelFilter;

pub fn init_logging(log_level: LevelFilter, other_log_level: LevelFilter) {
    let mut builder = Builder::from_env(Env::default());
    builder.target(Target::Stdout);
    builder.filter(None, other_log_level);
    builder.filter(Some("submerge_crystal_transfer"), log_level);
    builder.write_style(WriteStyle::Always);
    builder.init();
}