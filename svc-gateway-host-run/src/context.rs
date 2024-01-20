use crate::db as svc_db;
#[cfg(test)]
use mockall::mock;

pub struct MainContext {
  pub db: svc_db::MainContext,
}

impl MainContext {
  pub async fn wait(&self) {
    self.db.wait().await;
  }
}

pub trait DbContext
where
  Self::Inner: svc_db::Context,
{
  type Inner;

  fn db(&self) -> &Self::Inner;
}

#[cfg(test)]
mock! {
  pub DbContext {}
  impl DbContext for DbContext {
      type Inner = svc_db::MockContext;
      fn db(&self) -> &<Self as DbContext>::Inner;
  }
}

impl DbContext for MainContext {
  type Inner = svc_db::MainContext;

  fn db(&self) -> &Self::Inner {
    &self.db
  }
}
