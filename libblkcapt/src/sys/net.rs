use crate::runtime_dir;
use http::Request;
use hyper::{client::connect::dns::GaiResolver, client::HttpConnector, Client, Uri};
use hyper::{Body, Response};
use hyper_timeout::TimeoutConnector;
use hyper_tls::HttpsConnector;
use hyperlocal::UnixConnector;
use std::time::Duration;

type HyperClient = Client<TimeoutConnector<HttpsConnector<HttpConnector<GaiResolver>>>>;

pub struct HttpsClient {
    client: HyperClient,
}

impl HttpsClient {
    pub fn default() -> Self {
        let mut http = HttpConnector::new();
        http.set_connect_timeout(Some(Duration::from_secs(3)));
        http.enforce_http(false);
        let https = HttpsConnector::new_with_connector(http);
        let mut connector = TimeoutConnector::new(https);
        connector.set_read_timeout(Some(Duration::from_secs(5)));
        connector.set_write_timeout(Some(Duration::from_secs(5)));

        Self {
            client: Client::builder().build::<_, hyper::Body>(connector),
        }
    }

    pub async fn get(&self, url: Uri) -> Result<Response<Body>, hyper::Error> {
        self.client.get(url).await
    }

    pub async fn post(&self, url: Uri, body: String) -> Result<Response<Body>, hyper::Error> {
        let request = Request::post(url).body(Body::from(body)).expect("valid request setup");
        self.client.request(request).await
    }
}

pub struct ServiceClient {
    client: Client<TimeoutConnector<UnixConnector>>,
}

impl ServiceClient {
    pub fn default() -> Self {
        let unix = UnixConnector {};
        let mut connector = TimeoutConnector::new(unix);
        connector.set_read_timeout(Some(Duration::from_secs(10)));
        connector.set_write_timeout(Some(Duration::from_secs(10)));

        Self {
            client: Client::builder().build::<_, hyper::Body>(connector),
        }
    }

    pub async fn get(&self, path: &str) -> Result<Response<Body>, hyper::Error> {
        let socket_path = {
            let mut path = runtime_dir();
            path.push("daemon.sock");
            path
        };
        let url: Uri = hyperlocal::Uri::new(socket_path, path).into();
        self.client.get(url).await
    }
}
