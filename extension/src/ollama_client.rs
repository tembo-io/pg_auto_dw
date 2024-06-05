use pgrx::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct GenerateRequest {
    pub model: String,
    pub prompt: String,
    pub stream: bool,
}

#[derive(Deserialize, Debug)]
pub struct GenerateResponse {
    pub model: String,
    pub created_at: String,
    pub response: String,
    pub done: bool,
}

pub async fn send_request(new_json: &str) -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new();
    
    let prompt_template = r#"
    You are given a JSON object containing the schema name, table name, and details about each column in a table. This is source table information and downstream we are creating data vault tables. In this case, we are focusing on categorization for building hubs and satellites. Your task is to categorize each column into one of three categories: "Business Key," "Descriptor," or "Descriptor - Sensitive." The response should only include the column number and the category type for each column.
    
    Hard Rules:
    - If the column is a primary key, set "Category" to "Business Key."
    
    Example Input:
    
    {
      "Schema Name": "public",
      "Table Name": "customer",
      "Column Details": [
        "Column No: 1 Named: customer_id of type: uuid And is a primary key.",
        "Column No: 2 Named: city of type: character varying(255)",
        "Column No: 3 Named: state of type: character(2)",
        "Column No: 4 Named: zip of type: character varying(10)"
      ]
    }
    
    Expected Output:
    
    {
      "Schema Name": "public",
      "Table Name": "customer",
      "Column Details": [
        {
          "Column No": 1,
          "Category": "Business Key"
        },
        {
          "Column No": 2,
          "Category": "Descriptor"
        },
        {
          "Column No": 3,
          "Category": "Descriptor"
        },
        {
          "Column No": 4,
          "Category": "Descriptor - Sensitive"
        }
      ]
    }
    
    New JSON to Consider:
    
    {new_json}
    
    Please categorize the columns in the new JSON according to the following categories:
    - "Business Key": Identifiers or primary keys.
    - "Descriptor": General descriptive attributes.
    - "Descriptor - Sensitive": Sensitive information that needs to be handled with care.
    
    Return the output JSON with only the column number and the category type for each column.
    "#;

    // Inject new_json into the prompt_template
    let prompt = prompt_template.replace("{new_json}", new_json);

    let request = GenerateRequest {
        model: "mistral".to_string(),
        prompt,
        stream: false,
    };

    let response = client
        .post("http://localhost:11434/api/generate")
        .json(&request)
        .send()
        .await?
        .json::<GenerateResponse>()
        .await?;

    // Deserialize and pretty-print the response
    let response_json: serde_json::Value = serde_json::from_str(&response.response)?;
    log!("{}", serde_json::to_string_pretty(&response_json)?);

    Ok(())
}