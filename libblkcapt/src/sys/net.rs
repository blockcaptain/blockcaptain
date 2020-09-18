use hyper::{client::connect::dns::GaiResolver, client::HttpConnector, Client, Uri};
use hyper::{Body, Response};
use hyper_timeout::TimeoutConnector;
use hyper_tls::HttpsConnector;
use std::time::Duration;

// TODO: make a mockable trait for the static net client creator

type HyperClient = Client<TimeoutConnector<HttpsConnector<HttpConnector<GaiResolver>>>>;

pub struct HttpsClient {
    client: HyperClient,
}

impl HttpsClient {
    pub fn default() -> HttpsClient {
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(Duration::from_secs(3)));
        http.enforce_http(false);
        let https = HttpsConnector::new_with_connector(http);
        let mut connector = TimeoutConnector::new(https);
        connector.set_read_timeout(Some(Duration::from_secs(5)));
        connector.set_write_timeout(Some(Duration::from_secs(5)));

        HttpsClient {
            client: Client::builder().build::<_, hyper::Body>(connector),
        }
    }

    pub async fn get(&self, url: Uri) -> Result<Response<Body>, hyper::Error> {
        self.client.get(url).await
    }
}
