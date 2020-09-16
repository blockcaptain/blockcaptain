use hyper::{client::HttpConnector, Client, Uri};
use hyper::{Body, Response};
use hyper_timeout::TimeoutConnector;
use hyper_tls::HttpsConnector;
use std::time::Duration;

pub async fn https_request(url: Uri) -> Result<Response<Body>, hyper::Error> {
    let mut http = HttpConnector::new();
    http.set_connect_timeout(Some(Duration::from_secs(3)));
    http.enforce_http(false);
    let https = HttpsConnector::new_with_connector(http);
    let mut connector = TimeoutConnector::new(https);
    connector.set_read_timeout(Some(Duration::from_secs(5)));
    connector.set_write_timeout(Some(Duration::from_secs(5)));

    let client = Client::builder().build::<_, hyper::Body>(connector);
    client.get(url).await
}
