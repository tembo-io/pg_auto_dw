use pgrx::prelude::*;
use serde_json::json;
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
    // pub options: Options,
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

    // log!("Propmt: {prompt}");

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
        // options,
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
pub enum PromptTemplate {
    Standard,
    Incremental,
    IncrementalShort,
    BKIdentification,
    BKName,
    DescriptorSensitive,
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
            Category Value "Business Key Part": Hubs are associated with business keys and we're trying to understand if this column could be part of a unique identifier.  If so we want to categorize it as a business key part.  Very typically this is only one column at the beginning.  However, it could be two columns, typically at the beginning, but this is very unlikely.
            Category Value “Descriptor” or “Descriptor - Sensitive”: Satellites are associated with descriptors; that is, values not used to uniquely identify a business key.  If you think the column is descriptive of the business key and not used to create the unique business key itself it should be categorized as a “Descriptor”.  Further, if you think this may contain sensitive information, for example PII, then mark it as categorized as a “Descriptor - Sensitive”.

            Business Key Name Value: If you categorize it as a "Business Key Part” then you should also include the business key name, else provide "NA" as the value. The Business Key Name can be derived from the table name, from the attributes associated with the business key parts, and should reflect the entity generally. (Rules: The Business Key Name should not include terms like "ID", "number", or similar identifiers. It should express the business entity associated with the table, for example "customer," "employee," or "seller." The Business Key Name should also not include terms like “entity” or similar suffixes, and should only reflect the core business entity name.)

            Confidence Value: A two-decimal-place score which is a number between 0 to 1.  This represents your confidence value for this specific column response request.  Generally speaking, .80 or above is considered reasonably confident for this request.

            Reason: Indicate why you make the decision you made.

            Hints may be provided, you should take this into strong consideration for your response.

            Personally identifiable information, PII. A large subset of columns that could be classified as “Descriptor - Sensitive” are PII in nature.  Therefore, if you see columns that appear to be one of the following types in the list below then consider it PII and categorize it as “Descriptor - Sensitive”.
              - A Person's Name (first name or last name)
              - Social Security Number (SSN)
              - Driver's License Number
              - Passport Number
              - Email Address
              - Physical Street Address (street address, but not City, State, or standard zip)
              - Extended Zip (Zip code with more than 5 digits)
              - Telephone Number (landline and mobile)
              - Date of Birth
              - Place of Birth
              - Biometric Data (fingerprints, facial recognition data, iris scans)
              - Medical Information (health records, prescriptions)
              - Financial Information (bank account numbers, credit card numbers, debit card numbers)
              - Employment Information (employment records, salary information)
              - Insurance Information (policy numbers, claim information)
              - Education Records (student records, transcripts)
              - Online Identifiers (usernames, IP addresses, cookies, MAC addresses)
              - Photographs or videos that can identify an individual
              - National Identification Numbers (outside of the SSN, such as National Insurance Numbers in the UK)
              - Geolocation Data (GPS coordinates, location history)
              - Vehicle Registration Numbers

            Not PII
             - Zip with 5 Digits or less.

            Examples

            // Begining of Examples

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

            Hints: 

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

            Hints: 

            Expected Output:

            {
                "Column No": 2,
                "Category": "Descriptor",
                "Business Key Name": "NA",
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

            Hints: Column number more than 2, most likely a "Descriptor" or "Descriptor - Sensitive"

            Expected Example Output:

            {
                "Column No": 3,
                "Category": "Descriptor",
                "Business Key Name": "NA",
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

            Hints: Column number more than 2, most likely a "Descriptor" or "Descriptor - Sensitive"

            Expected Example Output:

            {
                "Column No": 4,
                "Category": "Descriptor - Sensitive",
                "Business Key Name": "NA",
                "Confidence": 0.90,
                "Reason": "The column 'zip' contains potentially sensitive information about the customer's location, which requires careful handling."
            }

            // End of Examples

            Now, based on the instructions and example above we are requesting a JSON to be returned given the following input.

            JSON Source Table Object: {new_json}

            Column Number: {column_number}

            Hints: {hints}

            "#,
          PromptTemplate::IncrementalShort => r#"
            You will receive a JSON source table object and a column number. Your task is to return a JSON response for creating data vault table structures.

            Required JSON Keys:

            •	Column No: The provided column number.
            •	Category: Categorize as “Business Key Part,” “Descriptor,” or “Descriptor - Sensitive.”
            •	Business Key Name: If “Business Key Part,” derive a name; otherwise, return “NA.”
            •	Confidence: A score between 0.00 and 1.00 indicating confidence.
            •	Reason: Explanation for your categorization.

            PII Consideration: If the column contains PII, categorize it as “Descriptor - Sensitive.” Examples of PII include names, SSNs, email addresses, etc.

            Examples:

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

            Hints: 

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

            Hints: 

            Expected Output:

            {
                "Column No": 2,
                "Category": "Descriptor",
                "Business Key Name": "NA",
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

            Hints:

            Expected Example Output:

            {
                "Column No": 3,
                "Category": "Descriptor",
                "Business Key Name": "NA",
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

            Hints:

            Expected Example Output:

            {
                "Column No": 4,
                "Category": "Descriptor - Sensitive",
                "Business Key Name": "NA",
                "Confidence": 0.90,
                "Reason": "The column 'zip' contains potentially sensitive information about the customer's location, which requires careful handling."
            }

            // End of Examples

            Now, based on the instructions and example above we are requesting a JSON to be returned given the following input.

            JSON Source Table Object: {new_json}

            Column Number: {column_number}

            Hints: {hints}

            "#,
          PromptTemplate::BKIdentification => r#"
            Task Title: Business Key Identification in JSON Source Table Object

            You have a JSON Source Table Object that includes the schema name, table name, and detailed column information. Your responses to requested tasks will be used to help create downstream data vault tables.

            Requested Task: Identify the column number most likely to serve as the business key. Return only one column in JSON format as specified below.


            Request Details:
            If the column is a primary key, assume it is the business key. If not, choose the column most likely to uniquely identify the table’s entity. Additionally, provide a confidence value for your selection.

            Confidence Value: Provide a score between 0 and 1, rounded to two decimal places, representing your confidence in the selected column. A value of 0.80 or higher is considered reasonably confident.


            Reason: Indicate why you made the decision you did.

            Output: Ensure the output conforms to the format shown in the examples below.

            Example Input 1)
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

            Example Output 1)
            {
              "Identified Business Key": {
                "Column No": 1,
                "Confidence Value": 0.95,
                "Reason": "The 'customer_id' column is designated as the primary key, which is typically the best candidate for a business key."
              }
            }

            Example Input 2)
            JSON Source Table Object:
            {
              "Schema Name": "sales",
              "Table Name": "order_details",
              "Column Details": [
                "Column No: 1 Named: order_id of type: integer Column Comments: NA",
                "Column No: 2 Named: product_id of type: integer Column Comments: NA",
                "Column No: 3 Named: quantity of type: integer Column Comments: NA",
                "Column No: 4 Named: order_date of type: date Column Comments: NA"
              ]
            }

            Example Output 2)
            {
              "Identified Business Key": {
                "Column No": 1,
                "Confidence Value": 0.75,
                "Reason": "Although 'order_id' is not explicitly marked as a primary key, it is likely to uniquely identify each order, making it a strong candidate for the business key."
              }
            }

            Now, based on the instructions and examples above, please generate the JSON output for the following input.

            JSON Source Table Object: {new_json}
            "#,
          PromptTemplate::BKName => r#"
            Task Title: Business Key Naming in JSON Source Table Object with specified Column

            You have a JSON Source Table Object that includes the schema name, table name, and detailed column information. Your responses to requested tasks will be used to help create downstream data vault tables.

            Requested Task: Identify the business key name.  The business key part column has already been identified, and its associated column number, “column no”, will be provided along with the JSON Source Table Object.  Return a name that best represents the business key from a data vault perspective.

            Request Details:

            The Business Key Name should be crafted based on the attribute linked to the business key, as identified by the provided column number. Prioritize the attribute name over the table name if the attribute name is descriptive enough. It should clearly represent the core business entity, avoiding generic terms like “ID,” “number,” or “Entity.” The name should focus solely on the business aspect, using terms like “customer,” “employee,” or “seller” that directly reflect the entity’s purpose, without unnecessary suffixes or identifiers. If the attribute associated with the business key or its column comments are not descriptive enough, the table name or schema name can be used to help formulate the Business Key Name.

            Confidence Value: Provide a score between 0 and 1, rounded to two decimal places, representing your confidence in your chosen Business Key Name. A value of 0.80 or higher is considered reasonably confident.


            Reason: Indicate why you made the decision you did.

            Output: Ensure the output conforms to the format shown in the examples below.

            Example Input 1)
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

            Column No: 1

            Example Output 1)
            {
              "Business Key Name": {
                "Name": "Customer",
                "Confidence Value": 0.9,
                "Reason": "The column 'customer_id' is a primary key and represents the unique identifier for customers in the 'customer' table. Given that the table name 'customer' directly reflects the business entity, 'Customer' is chosen as the Business Key Name. The confidence value is high because the identifier is straightforward and strongly aligned with the core business entity."
              }
            }

            Example Input 2)
            JSON Source Table Object:
            {
              "Schema Name": "sales",
              "Table Name": "order_details",
              "Column Details": [
                "Column No: 1 Named: id of type: integer Column Comments: NA",
                "Column No: 2 Named: product_id of type: integer Column Comments: NA",
                "Column No: 3 Named: quantity of type: integer Column Comments: NA",
                "Column No: 4 Named: order_date of type: date Column Comments: NA"
              ]
            }

            Column No: 1

            Example Output 2)
            {
              "Business Key Name": {
                "Name": "Order",
                "Confidence Value": 0.85,
                "Reason": "The column 'id' is a primary key and serves as the unique identifier for records in the 'order_details' table. Although the column name 'id' is generic, the table name 'order_details' indicates that the records pertain to individual orders. Therefore, 'Order' is chosen as the Business Key Name to best represent the core business entity. The confidence value is slightly lower due to the generic nature of the column name, but it is still reasonably confident given the context provided by the table name."
              }
            }

            Now, based on the instructions and examples above, please generate the JSON output for the following input.

            JSON Source Table Object: {new_json}

            Column No: {column_no}
            "#,
          PromptTemplate::DescriptorSensitive => r#"
            Task Title: Identification of PII in JSON Source Table Object

            You have a JSON Source Table Object that includes the schema name, table name, and detailed column information. Your task is to assist in the creation of downstream data vault tables by performing the requested tasks based on this information.

            Requested Task: Identify if the descriptor is a descriptor sensitive PII subtype. A descriptor column, along with its associated column number (“column no”), will be provided in the JSON Source Table Object. If you determine that the column contains Personally Identifiable Information (PII), categorize it as “Descriptor - Sensitive.”

            Request Details:
            PII Identification: Only consider a column as PII if it directly matches an item from the PII list provided below. Do not infer or project beyond this list. If a column name or its associated comment closely resembles an item from the list, classify it as PII.
            No Overgeneralization: Avoid overgeneralization or inference beyond what is explicitly stated in the list. Focus strictly on the provided PII list.

            Personal Identifiable Information (PII) List:

            Consider any of the following types of information as PII and categorize the corresponding column as “Descriptor - Sensitive”:

            - Person’s Name: PII (Includes first name, last name, or both).
            - Social Security Number (SSN): PII
            - Driver’s License Number: PII
            - Passport Number: PII
            - Email Address: PII
            - Physical Street Address: PII (Includes street address, but excludes City, State, or standard 5-digit Zip code).
            - Extended Zip Code: PII (Any Zip code with more than 5 digits).
            - Telephone Number: PII (Includes both landline and mobile numbers).
            - Date of Birth: PII
            - Place of Birth: PII
            - Biometric Data: PII (Includes fingerprints, facial recognition data, iris scans).
            - Medical Information: PII (Includes health records, prescriptions).
            - Financial Information: PII (Includes bank account numbers, credit card numbers, debit card numbers).
            - Employment Information: PII (Includes employment records, salary information).
            - Insurance Information: PII (Includes policy numbers, claim information).
            - Education Records: PII (Includes student records, transcripts).
            - Online Identifiers: PII (Includes usernames, IP addresses, cookies, MAC addresses).
            - Photographs or Videos: PII (Any media that can identify an individual).
            - National Identification Numbers: PII (Includes identifiers outside of SSN, such as National Insurance Numbers in the UK).
            - Geolocation Data: PII (Includes GPS coordinates, location history).
            - Vehicle Registration Numbers: PII

            Not PII:

            Some data may seem personally identifiable; however, it is not specific enough to identify an individual.

            - Standard 5-Digit Zip Code: Not PII
            - City: Not PII
            - State: Not PII
            - Country: Not PII
            - Age (in years): Not PII (Unless combined with other identifiers like date of birth).
            - Gender: Not PII
            - Ethnicity/Race: Not PII (General categories, e.g., “Caucasian,” “Asian,” without additional identifiers).
            - Publicly Available Information: Not PII (Any information that is lawfully made available from federal, state, or local government records).
            - Generic Job Titles: Not PII (Titles like “Manager,” “Engineer,” without additional identifying details).
            - Company/Organization Name: Not PII (Names of companies or organizations without personal identifiers).

            Confidence Value: Provide a score between 0 and 1, rounded to two decimal places, representing your confidence in your “Is PII” determination of true or false. A value of 0.80 or higher is considered reasonably confident in your true or false answer.


            Reason: Indicate why you made the decision you did.

            Output: Please ensure that your output is JSON and matches the structure of the output examples provided.

            Example Input 1)
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

            Column No: 4

            Example Output 1)
            {
              "Descriptor - Sensitive": {
                "Is PII": true,
                "Confidence Value": 0.85,
                "Reason": "The 'zip' column is identified as PII because its data type, character varying(10), allows for the possibility of storing extended zip codes, which matches an item on the provided PII list."
              }
            }

            Example Input 2)
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

            Column No: 2

            Example Output 2)
            {
              "Descriptor - Sensitive": {
                "Is PII": false,
                "Confidence Value": 0.90,
                "Reason": "The 'city' column is not considered PII because city names do not match any item on the provided PII list."
              }
            }

            Example Input 3)
            JSON Source Table Object:
            {
              "Schema Name": "public",
              "Table Name": "employee",
              "Column Details": [
                "Column No: 1 Named: employee_id of type: uuid And is a primary key.  Column Comments: NA",
                "Column No: 2 Named: full_name of type: character varying(255) Column Comments: NA",
                "Column No: 3 Named: email of type: character varying(255) Column Comments: NA",
                "Column No: 4 Named: salary of type: numeric Column Comments: NA"
              ]
            }

            Column No: 2

            Example Output 3)
            {
              "Descriptor - Sensitive": {
                "Is PII": true,
                "Confidence Value": 0.95,
                "Reason": "The 'full_name' column is identified as PII because it matches the 'Person's Name' item from the provided PII list."
              }
            }

            Example Input 4)
            JSON Source Table Object:
            {
              "Schema Name": "public",
              "Table Name": "order",
              "Column Details": [
                "Column No: 1 Named: order_id of type: uuid And is a primary key.  Column Comments: NA",
                "Column No: 2 Named: order_date of type: date Column Comments: NA",
                "Column No: 3 Named: customer_email of type: character varying(255) Column Comments: 'Email address of the customer who placed the order'",
                "Column No: 4 Named: total_amount of type: numeric Column Comments: NA"
              ]
            }

            Column No: 3

            Example Output 4)
            {
              "Descriptor - Sensitive": {
                "Is PII": true,
                "Confidence Value": 0.98,
                "Reason": "The 'customer_email' column is identified as PII because it matches the 'Email Address' item from the provided PII list."
              }
            }

            Now, based on the instructions and examples above, please generate the appropriate JSON output only for the following JSON Source Table Object and Column No inputs. Please ensure that the JSON output adheres exactly to both the format and content structure shown in the examples above.

            JSON Source Table Object: {new_json}

            Column No: {column_no}

            "#,
      }
  }
}


