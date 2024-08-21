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

pub async fn send_request(new_json: &str, col: u32) -> Result<serde_json::Value, Box<dyn std::error::Error>> {

    let client = ClientBuilder::new().timeout(Duration::from_secs(180)).build()?; // 30 sec Default to short for some LLMS.
    
    let prompt_template = PromptTemplate::Incremental.template();

    // Inject new_json into the prompt_template'
    let column_number = col.to_string();
    let prompt = prompt_template
                          .replace("{new_json}", new_json)
                          .replace("{column_number}", &column_number);

    // log!("Propmt: {prompt}");

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
    
    // log!("JSON Reply: {response_json}");

    Ok(response_json)
}

#[derive(Debug)]
enum PromptTemplate {
    Standard,
    Incremental,
}

impl PromptTemplate {
  fn template(&self) -> &str {
      match self {
          PromptTemplate::Standard => r#"
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
            
            Hard Rules: 
            1) Only categorize these into 3 distinct categories listed above and here, "Business Key Part", "Descriptor", or "Descriptor - Sensitive".  DO NOT RETURN categories not on this list like "Descriptor - Timestamp".
            2) You should return as many columns back as is given without consolidation.  And none should be skipped.
            3) Only one attribute should be associate with a business key and typically it's at the beginning.  For example column number 10, username, should not be consider a business key.
            
            Return the output JSON with the column number, the category type, a confidence score, and reason for each column. Plus, if the category is a business key part, provide a business key name at the attribute level.  The business key name should be derived from the table name and the attributes associated with the business key parts.  The name should exclude terms like "ID," "number," and "Entity," and reflecting only the core business entity name.  If the category is not a business key part specify "Business Key Name: "NA" as the example above shows.

            And AGAIN there are only 3 categories, "Business Key Part", "Descriptor", or "Descriptor - Sensitive".  If you think the answer is "Descriptor - Timestamp" that is incorrect and most like should just be "Descriptor".
            "#,
          PromptTemplate::Incremental => r#"
            You will be provided with a JSON source table object and a column number. The JSON source table object will contain a source table's schema name, table name, and details about each column in a table. You will deliver a JSON response for our system to create downstream data vault table structures.

            Requested JSON Response:

            Keys

            Given a JSON source table object and a column number, we request 5 pieces of information delivered as keypairs.
            "Column No"
            "Category"
            "Business Key Name"
            "Confidence"
            "Reason"

            Values

            Column No Value: This is the column number we provided you, relating to a specific attribute, which you should return.  It serves as an identifier.

            Category Value: In this case, we are focusing on categorization of attributes for building hubs and satellites.  To do this we want to categorize each column into one of only three categories: "Business Key Part," "Descriptor," or "Descriptor - Sensitive".

            - Category Value "Business Key Part": Hubs are associated with business keys and we’re trying to understand if this column could be part of a unique identifier.  If so we want to categorize it as a business key part.  Very typically this is only one column at the beginning.  However, it could be two columns, typically at the beginning, but this is very unlikely.

            - Category Value “Descriptor” or “Descriptor - Sensitive”: Satellites are associated with descriptors; that is, values not used to uniquely identify a business key.  If you think the column is descriptive of the business key and not used to create the unique business key itself it should be categorized as a “Descriptor”.  Further, if you think this may contain sensitive information, for example PII, then mark it as categorized as a “Descriptor - Sensitive”.
            
            Business Key Name Value: If you categorize it as a "Business Key Part” then you should also include the business key name, else provide "NA" as the value. The Business Key Name can be derived from the table name, from the attributes associated with the business key parts, and should reflect the entity generally. (Rules: The Business Key Name should not include terms like "ID", "number", or similar identifiers. It should express the business entity associated with the table, for example "customer," "employee," or "seller." The Business Key Name should also not include terms like “entity” or similar suffixes, and should only reflect the core business entity name.)

            Confidence Value: A two-decimal-place score which is a number between 0 to 1.  This represents your confidence value for this specific column response request.  Generally speaking, .80 or above is considered reasonably confident for this request.

            Reason: Indicate why you make the decision you made.

            Examples

            // Begin of Examples
            Example 1)

            JSON Source Table Object:

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

            Column Number: 1

            Expected Example Output:

            {
              "Column No": 1,
              "Category": "Business Key Part",
              "Business Key Name": "customer",
              "Confidence": 0.99,
              "Reason": "The column 'customer_id' is a primary key, which is a strong indicator of a Business Key."
            }

            Example 2)

            JSON Source Table Object:

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

            Column Number: 2

            Expected Output:

            {
                "Column No": 2,
                "Category": "Descriptor",
                "Business Key Name: "NA",
                "Confidence": 0.85,
                "Reason": "The column 'city' provides general descriptive information about the customer, which is typically a Descriptor."
            }

            Example 3)

            JSON Source Table Object:

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

            Column Number: 3

            Expected Example Output:

            {
                "Column No": 3,
                "Category": "Descriptor",
                "Business Key Name: "NA",
                "Confidence": 0.80,
                "Reason": "The column 'state' provides general descriptive information and is less likely to be sensitive, hence categorized as a Descriptor."
            }

            Example 4)

            JSON Source Table Object:

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

            Column Number: 4

            Expected Example Output:

            {
                "Column No": 4,
                "Category": "Descriptor - Sensitive",
                "Business Key Name: "NA",
                "Confidence": 0.90,
                "Reason": "The column 'zip' contains potentially sensitive information about the customer's location, which requires careful handling."
            }

            // End of Examples

            Now, based on the instructions and example above we’re requesting a JSON to be returned given the following input.

            JSON Source Table Object: {new_json}

            Column Number: {column_number}
            "#,
      }
  }
}