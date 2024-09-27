use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::utility::guc;
use crate::model::prompt_template::PromptTemplate;

#[derive(Serialize, Debug)]
pub struct Request {
    pub model: String,               // Model name for OpenAI
    pub messages: Vec<Message>,      // List of messages for chat format
    pub temperature: f64,            // Temperature setting
    pub response_format: ResponseFormat,  // JSON-only response format field
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Message {
    pub role: String,                // "user", "assistant", or "system"
    pub content: String,             // The actual prompt or message content
}

#[derive(Serialize, Debug)]
pub struct ResponseFormat {
    #[serde(rename = "type")] 
    pub r#type: String,              // To ensure JSON response format
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub id: String,                 // Unique identifier for the chat session
    pub object: String,             // Object type, usually "chat.completion"
    pub created: u64,               // Timestamp when the response was created
    pub model: String,              // Model name used for the response
    pub choices: Vec<Choice>,       // List of choices (contains the actual answer)
    pub usage: Usage,               // Information about token usage
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Choice {
    pub message: Message,           // Contains the assistant's message
    pub finish_reason: Option<String>, // Reason for stopping (e.g., "stop")
    pub index: usize,               // Index of the choice
    pub logprobs: Option<serde_json::Value>, // Log probabilities (if applicable)
}


#[derive(Serialize, Deserialize, Debug)]
pub struct Usage {
    pub prompt_tokens: u32,         // Number of tokens in the prompt
    pub completion_tokens: u32,     // Number of tokens in the completion
    pub total_tokens: u32,          // Total number of tokens used
}

pub async fn send_request(new_json: &str, template_type: PromptTemplate, col: &u32, hints: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {

    let client = ClientBuilder::new().timeout(Duration::from_secs(60)).build()?; // 30 sec Default to short for some LLMS.
    
    let prompt_template = template_type.template();
    // let prompt_template = PromptTemplate::Test.template();

    // Inject new_json into the prompt_template'
    let column_number = col.to_string();
    let prompt = prompt_template
                          .replace("{new_json}", new_json)
                          .replace("{column_no}", &column_number)
                          .replace("{hints}", &hints);  

    // GUC Values for the transformer server
    let transformer_server_url = guc::get_guc(guc::PgAutoDWGuc::TransformerServerUrl).ok_or("GUC: Transformer Server URL is not set.")?;
    let transformer_server_token = guc::get_guc(guc::PgAutoDWGuc::TransformerServerToken).ok_or("GUC: Transformer Server Token is not set.")?;

    let model = guc::get_guc(guc::PgAutoDWGuc::Model).ok_or("MODEL GUC is not set.")?;
    
    let json_type = String::from("json_object");
    let response_format = ResponseFormat { r#type: json_type,};

    let temperature: f64 = 0.75;

    let role = String::from("user");

    let message = Message {
        role,
        content: prompt,
    };

    let messages = vec![message];

    let request = Request {
        model,
        messages,
        temperature,
        response_format,
    };

    let response = client
        .post(&transformer_server_url)  // Ensure this is updated to OpenAI's URL
        .header("Authorization", format!("Bearer {}", transformer_server_token))  // Add Bearer token here
        .header("Content-Type", "application/json")  // Specify JSON content type
        .json(&request)  // Send the request body as JSON
        .send()
        .await?
        .json::<Response>()  // Await the response and parse it as JSON
        .await?;

    // Extract the content string
    let content_str = &response
        .choices
        .get(0)
        .ok_or("No choices in response")?
        .message
        .content;

    // Parse the content string into serde_json::Value
    let content_json: serde_json::Value = serde_json::from_str(content_str)?;

    Ok(content_json)
}




