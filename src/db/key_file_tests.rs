//! A service account key passed to the client as `google-cloud-auth` credentials.
//!
//! Signing with the key needs a rustls crypto provider, which the default features carry. A
//! build without `auth-default-crypto` leaves installing one to the application, and fails here.

use super::*;
use crate::db::fake::{FakeBigQuery, FakeCall};
use gcloud_sdk::google::cloud::bigquery::v2::{GetTableRequest, Table};
use gcloud_sdk::google_cloud_auth::credentials::service_account;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use std::time::Duration;

/// A 2048-bit RSA key generated for this test; it signs nothing outside it.
const TEST_RSA_PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC636zjJMIs+lhs
i4jfv4w5EWm3fnnlEbtjQFj2yOVI7vwV8M+dALbNbPR6DNBX5m59Ijj5/pt6HARw
ZM8JyOIEcb17N1hkQRv59+r/ZCa7j+odi25lSsHpQ9SOTT2CH7LwvdKd3vWsVC8e
tueAxBxa6tFgYVs3ZHvK2E5jzF5wtauSCbmt42G6YFJVz+zGgdSqXQQPwr6Z7sfl
ZdkjrSOGjDYjTr36UQsYLunXrHlqELoBl4Qri0SX4ZZCHQSRk5C/3P48jjjfhMCx
ow0QlYaHF/Nt4UHcUpZ+N1njug9Z6X0ru1LZGRPKngTbBMssKeMAJiBBFyDG/XUX
YpQNZ2E5AgMBAAECggEABx0zRiK/hU9S1O+wmlX+3FWk85BBdd6UMT3Wn9nQ3WiD
A8HQYYl7VrwxCBbF9VwHdO+RLDaMttwWBi0Bnb1+RI2LmVh8yV+aKqINGe5h3sOR
NdE5oWpfEfExXb55MDjiisbV27PhyzmZi/Q0wXfrHWycORdTuJA0eenR/Xt0VUWL
P4izfQItv/X/PkiTOlEVzEN9F3StxWkDOge1Uz9wuS7NnOH0XAc0dX5JKjmLR3Oc
bE/6Nag4ipx0gLfhjZNwsXCeIlB+bt3hrVwdzVev8SSul3EmcWBJatiC8pAxmuea
w6aWXujjiKHaiqTiRsY5ZkxysnrZdZ3p8Q812amxVQKBgQDiGjz6pEdFXb0W2Ukv
Vo+h1juJTZY8rOT+Y8EbsyTr/00yHSQYVq7a0bmAWI7sg2eQJeirWEFxK1LrxZSH
qhNNF2S04sbA23UBuUYD5ieiClGDMAA/BRDVaMjNdaDV7I4KqOjQ89yk+AAltAN1
Alsg6GjvN1VbphZh37e6tkBP/QKBgQDTlYKb9USB/51aqvMgtIQy6Z1v4z72XEnG
dT47UbjmLDQz8uupyb79HO/XmDLrEg70oRP5XTf3/LBGlEPH2xPZBptnwWaZFCW3
gZtG5/liq7BT7RWECh5d7K2HWhEZ2Qpi4ThppVpH3Q0ziiZgvc2uFxrUtv+OIj2y
R9r7USDk7QKBgQC9FKAmIrePZMK6lt9C/SqEdnm3avDWsOeXbaiSF5Aq44eU/xWu
XN9BEt4EXQq483Lp8ynOELI5h7eCf+qkQTB+6a9ILdV0dpI07NGGdTRzqBlekgfC
q+PV9Bjo6ROlknt+op6AD09XjiXhYdb6RQwcSv9l/WoeiJhzJPmq5LqHqQKBgQCM
fkF8x47W92GH++128Qa7O86TpgR/1+YRBnbtMqWhCQcc25fKZTERpy5HLUPaPWJ6
4558vX5Vh4+RPhAH3BvAxxdX/N7glLU6ta/ilFRPRYvPHqVjyMhDO846Cv9B95Fi
+QPZereI8mo6p+g14rJ5f9QMHPtyaKApCneI22UJPQKBgGfLvkUnnb4X3cHbLbec
yw63Lgm5IIt0QHZePCjwyUhJmkRUWpLiuM1Ngt3ogyzDx3Y/Nk6OaBHB4Sq87oc/
5I3cnT/1G33RDqEzAAKQqAjvmQcQB4nUnBt1U8CQbCsnR2G8eo2BUABZ3CSirgrw
TlMXJJWmFfFfEkoozDyE+LWo
-----END PRIVATE KEY-----";

/// The public half of [`TEST_RSA_PRIVATE_KEY`].
const TEST_RSA_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAut+s4yTCLPpYbIuI37+M
ORFpt3555RG7Y0BY9sjlSO78FfDPnQC2zWz0egzQV+ZufSI4+f6behwEcGTPCcji
BHG9ezdYZEEb+ffq/2Qmu4/qHYtuZUrB6UPUjk09gh+y8L3Snd71rFQvHrbngMQc
WurRYGFbN2R7ythOY8xecLWrkgm5reNhumBSVc/sxoHUql0ED8K+me7H5WXZI60j
how2I069+lELGC7p16x5ahC6AZeEK4tEl+GWQh0EkZOQv9z+PI4434TAsaMNEJWG
hxfzbeFB3FKWfjdZ47oPWel9K7tS2RkTyp4E2wTLLCnjACYgQRcgxv11F2KUDWdh
OQIDAQAB
-----END PUBLIC KEY-----";

const SERVICE_ACCOUNT: &str = "bigquery-test@fake-project.iam.gserviceaccount.com";
const KEY_ID: &str = "test-key-id";

/// The claims that tie a self-signed JWT to the service account that signed it.
#[derive(Debug, PartialEq, serde::Deserialize)]
struct SignerClaims {
    iss: String,
    sub: String,
}

#[tokio::test]
async fn a_service_account_key_signs_the_bearer_token_the_client_sends() {
    let fake = FakeBigQuery::start(|mut call: FakeCall| async move {
        let _: GetTableRequest = call.next_request().await.expect("a GetTable request");
        call.log(call.header("authorization").unwrap_or_default());
        call.reply(&Table::default());
    })
    .await;
    let key = serde_json::json!({
        "type": "service_account",
        "project_id": "fake-project",
        "private_key_id": KEY_ID,
        "private_key": TEST_RSA_PRIVATE_KEY,
        "client_email": SERVICE_ACCOUNT,
    });
    let credentials = service_account::Builder::new(key)
        .build()
        .expect("credentials from the service account key");
    let db = BigQueryDb::with_options_auth(fake.db.options().clone(), credentials)
        .await
        .expect("a client with service account key credentials");

    tokio::time::timeout(
        Duration::from_secs(10),
        db.table_client().get_table(GetTableRequest::default()),
    )
    .await
    .expect("the call completes")
    .expect("the fake server answers");

    let [authorization] = fake.calls().try_into().expect("one call");
    let token = authorization
        .strip_prefix("Bearer ")
        .expect("a bearer token");
    let header = jsonwebtoken::decode_header(token).expect("a JWT header");
    assert_eq!(
        (header.alg, header.kid.as_deref()),
        (Algorithm::RS256, Some(KEY_ID))
    );
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_required_spec_claims(&["exp", "iss", "sub"]);
    let public_key =
        DecodingKey::from_rsa_pem(TEST_RSA_PUBLIC_KEY.as_bytes()).expect("an RSA public key");
    let claims = jsonwebtoken::decode::<SignerClaims>(token, &public_key, &validation)
        .expect("a JWT signed by the key")
        .claims;
    assert_eq!(
        claims,
        SignerClaims {
            iss: SERVICE_ACCOUNT.to_string(),
            sub: SERVICE_ACCOUNT.to_string(),
        }
    );
}
