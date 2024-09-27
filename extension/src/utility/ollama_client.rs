use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::utility::guc;
use crate::model::prompt_template::PromptTemplate;

#[derive(Serialize, Debug)]
pub struct GenerateRequest {
    pub model: String,
    pub prompt: String,
    pub format: String,
    pub stream: bool,
    pub options: Options,
}

#[derive(Serialize, Debug)]
pub struct Options {
  pub temperature: f64,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct GenerateResponse {
    pub model: String,
    pub created_at: String,
    pub response: String,
    pub done: bool,
}

pub async fn send_request(new_json: &str, template_type: PromptTemplate, col: &u32, hints: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {

    let client = ClientBuilder::new().timeout(Duration::from_secs(180)).build()?; // 30 sec Default to short for some LLMS.
    
    let prompt_template = template_type.template();

    // Inject new_json into the prompt_template'
    let column_number = col.to_string();
    let prompt = prompt_template
                          .replace("{new_json}", new_json)
                          .replace("{column_no}", &column_number)
                          .replace("{hints}", &hints);  

    // GUC Values for the transformer server
    let transformer_server_url = guc::get_guc(guc::PgAutoDWGuc::TransformerServerUrl).ok_or("GUC: Transformer Server URL is not set")?;
    let model = guc::get_guc(guc::PgAutoDWGuc::Model).ok_or("MODEL GUC is not set.")?;

    let temperature: f64 = 0.75;

    let options: Options = Options{
      temperature,
    };

    let request = GenerateRequest {
        model,
        prompt,
        format: "json".to_string(),
        stream: false,
        options,
    };

    let response = client
        .post(&transformer_server_url)
        .json(&request)
        .send()
        .await?
        .json::<GenerateResponse>()
        .await?;

    // Deserialize
    let response_json: serde_json::Value = serde_json::from_str(&response.response)?;

    Ok(response_json)
}
