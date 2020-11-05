use crate::xactorext::{BcActor, BcActorCtrl};
use anyhow::Result;
use slog::Logger;
use xactor::{message, Context, Service};

pub struct IntelActor {}

#[message()]
pub struct ActorStart();

#[message()]
pub struct ActorStop();

impl IntelActor {
    pub fn new(log: &Logger) -> BcActor<Self> {
        BcActor::new(Self {}, log)
    }
}

#[async_trait::async_trait]
impl BcActorCtrl for IntelActor {
    async fn started(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) -> Result<()> {
        Ok(())
    }

    async fn stopped(&mut self, _log: &Logger, ctx: &mut Context<BcActor<Self>>) {}
}

impl Default for BcActor<IntelActor> {
    fn default() -> Self {
        IntelActor::new(&slog_scope::logger())
    }
}

impl Service for BcActor<IntelActor> {}
