pub mod search_query;
pub mod search_update;

use crate::core::result::DynResult;
use crate::db;
use crate::search;
use std::env;
use std::net;
use std::sync::Arc;
use tokio::spawn;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::try_join;

#[derive(Clone)]
pub struct Context(Arc<InnerContext>);

impl std::ops::Deref for Context {
  type Target = Arc<InnerContext>;

  fn deref(&self) -> &Self::Target {
    &self.0
  }
}

pub struct InnerContext {
  pub cpu_available: Semaphore,
  pub search_update_requested: Notify,
}

pub async fn run(
  search_context_map: search::ContextMap,
  db_context: db::Context,
  db_handle: db::Handle,
) -> DynResult<()> {
  let api_context = Context(Arc::new(InnerContext {
    cpu_available: Semaphore::new(num_cpus::get()),
    search_update_requested: Notify::new(),
  }));
  let periodic_search_update_handle =
    spawn(search_update::periodic(api_context.clone()));
  let run_search_update_handle = spawn(search_update::run(
    api_context.clone(),
    search_context_map.clone(),
    db_context.clone(),
  ));
  let run_server_handle = spawn(run_server(
    api_context.clone(),
    search_context_map.clone(),
  ));
  try_join!(
    periodic_search_update_handle,
    run_search_update_handle,
    run_server_handle,
    db_handle.connection_handle
  )?;
  Ok(())
}

async fn run_server(
  api_context: Context,
  search_context_map: search::ContextMap,
) {
  let result: DynResult<()> = (|| async {
    let host: net::Ipv4Addr =
      env::var("FTS_HOST")?.parse()?;
    let port: u16 = env::var("FTS_PORT")?.parse()?;
    let addr = net::SocketAddr::from((host, port));
    let service = hyper::service::service_fn(move |req| {
      handle_request(
        api_context.clone(),
        search_context_map.clone(),
        req,
      )
    });
    let server = hyper::Server::bind(&addr)
      .serve(tower::make::Shared::new(service));
    server.await?;
    Ok(())
  })()
  .await;
  result.expect("server error");
}

async fn handle_request(
  api_context: Context,
  search_context_map: search::ContextMap,
  req: hyper::Request<hyper::Body>,
) -> DynResult<hyper::Response<hyper::Body>> {
  let result =
    match (req.method().as_str(), req.uri().path()) {
      ("GET", "/") => Ok(hyper::Response::new("OK".into())),
      ("GET", "/query") => {
        search_query::handle(
          api_context,
          search_context_map,
          req,
        )
        .await
      }
      ("POST", "/update") => {
        search_update::handle(api_context)
      }
      _ => Ok(
        hyper::Response::builder()
          .status(hyper::StatusCode::NOT_FOUND)
          .body("not found".into())?,
      ),
    };
  match result {
    Err(err) => {
      eprintln!("HTTP error {}", err);
      Ok(
        hyper::Response::builder()
          .status(hyper::StatusCode::INTERNAL_SERVER_ERROR)
          .body("error".into())?,
      )
    }
    _ => result,
  }
}
