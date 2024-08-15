use pgrx::prelude::*;
use reqwest::ClientBuilder;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::utility::guc;

#[derive(Serialize, Debug)]
pub struct GenerateRequest {
    pub model: String,
    pub prompt: String,
    pub format: String,
    pub stream: bool,
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
pub struct GenerateResponse {
    pub model: String,
    pub created_at: String,
    pub response: String,
    pub done: bool,
}

pub async fn send_request(new_json: &str) -> Result<serde_json::Value, Box<dyn std::error::Error>> {

    let client = ClientBuilder::new().timeout(Duration::from_secs(180)).build()?; // 30 sec Default to short for some LLMS.
    
    let prompt_template = r#"
      You are given a JSON object containing the schema name, table name, and details about each column in a table. This is source table information and downstream we are creating data vault tables. In this case, we are focusing on categorization for building hubs and satellites. Your task is to categorize each column into one of three categories: "Business Key Part," "Descriptor," or "Descriptor - Sensitive." The response should include the column number, the category type, and a confidence score for each categorization. 
      
      Additionally, you should also include the business key name assoicated with the attributes that are part of the busines key, i.e. category "Business Key Part."  The Business Key Name should be derived from the table name and the attributes associated with the business key parts, and it should reflect the entity described by the table. The Business Key Name should not include terms like "ID," "number," or similar identifiers. It should express the business entity associated with the table, such as "customer," "employee," or "seller." The Business Key Name should also not include terms like "Entity" or similar suffixes, and should only reflect the core business entity name.

      Hard Rules:
      - If the column is a primary key, set "Category" to "Business Key Part."

      Example Input:

      {
        "Schema Name": "public",
        "Table Name": "customer",
        "Column Details": [
          "Column No: 1 Named: customer_id of type: uuid And is a primary key.  Column Comments: NA",
          "Column No: 2 Named: city of type: character varying(255) Column Comments: NA",
          "Column No: 3 Named: state of type: character(2) Column Comments: NA",
          "Column No: 4 Named: zip of type: character varying(10) Column Comments: NA"
        ]
      }

      Expected Output:
      
      {
      "Schema Name": "public",
      "Table Name": "customer",
      "Column Details": [
          {
          "Column No": 1,
          "Category": "Business Key Part",
          "Business Key Name: "customer",
          "Confidence": 0.99,
          "Reason": "The column 'customer_id' is a primary key, which is a strong indicator of a Business Key."
          },
          {
          "Column No": 2,
          "Category": "Descriptor",
          "Business Key Name: "NA",
          "Confidence": 0.85,
          "Reason": "The column 'city' provides general descriptive information about the customer, which is typically a Descriptor."
          },
          {
          "Column No": 3,
          "Category": "Descriptor",
          "Business Key Name: "NA",
          "Confidence": 0.80,
          "Reason": "The column 'state' provides general descriptive information and is less likely to be sensitive, hence categorized as a Descriptor."
          },
          {
          "Column No": 4,
          "Category": "Descriptor - Sensitive",
          "Business Key Name: "NA",
          "Confidence": 0.90,
          "Reason": "The column 'zip' contains potentially sensitive information about the customer's location, which requires careful handling."
          }
      ]
      }

      New JSON to Consider:

      {new_json}

      Categorize the columns in the new JSON according to the following categories only:
      - "Business Key Part": Identifiers or primary keys.
      - "Descriptor": General descriptive attributes.
      - "Descriptor - Sensitive": Sensitive information that needs to be handled with care.
      
      If you have qualifiers, like it would be a "Descriptor - Sensitive" if this case is true.  Lower your confidence score and include that in the reasons.
      
      Hard Rule: Only categorize these into 3 distinct categories listed above and here, "Business Key Part", "Descriptor", or "Descriptor - Sensitive".  DO NOT RETURN categories not on this list like "Descriptor - Timestamp".

      Return the output JSON with the column number, the category type, a confidence score, and reason for each column. Plus, if the category is a business key part, provide a business key name at the attribute level.  The business key name should be derived from the table name and the attributes associated with the business key parts.  The name should exclude terms like "ID," "number," and "Entity," and reflecting only the core business entity name.  If the category is not a business key part specify "Business Key Name: "NA" as the example above shows.

      And AGAIN there are only 3 categories, "Business Key Part", "Descriptor", or "Descriptor - Sensitive".  If you think the answer is "Descriptor - Timestamp" that is incorrect and most like should just be "Descriptor".
      "#;

    // Inject new_json into the prompt_template
    let prompt = prompt_template.replace("{new_json}", new_json);

    log!("JSON: {new_json}");

    // GUC Values for the transformer server
    let transformer_server_url = guc::get_guc(guc::PgAutoDWGuc::TransformerServerUrl).ok_or("GUC: Transformer Server URL is not set")?;
    let model = guc::get_guc(guc::PgAutoDWGuc::Model).ok_or("MODEL GUC is not set.")?;

    let request = GenerateRequest {
        model,
        prompt,
        format: "json".to_string(),
        stream: false,
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
    log!("JSON: {response_json}");

    Ok(response_json)
}