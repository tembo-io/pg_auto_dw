#[derive(Debug)]
pub enum PromptTemplate {
    BKComponentIdentification,
    BKName,
    DescriptorSensitive,
}

impl PromptTemplate {
  pub fn template(&self) -> &str {
      match self {
          PromptTemplate::BKComponentIdentification => r#"
            Task Title: Business Key Component Identification by Column in JSON Source Table Object

            You have a JSON Source Table Object that includes the schema name, table name, and detailed column information. Your task is to evaluate if a specified column is a business key component and, if so, how likely it is. The results of your evaluations will be used to create downstream data vault structures.

            A business key component is an attribute that forms part of a business key, which may be either a component of a composite key or a single key that uniquely identifies the record set. Additionally, there may be multiple business keys within one table.

            Requested Task:

            Determine whether the specified column, identified by its column number (“column no”), is likely to represent a business key or a component of a business key.

            Request Details:

            If the column is a primary key, as indicated in the comments or column details, assume it is a business key component. However, this does not exclude the possibility of other business key components within the table, but it may reduce the likelihood of the specified column being the only business key.

            If the specified column could be categorized as an email or username, only consider it a business key component if there are no other attributes in the table that could reasonably serve as a business key component.

            Confidence Value:

            Provide a confidence score between 0 and 1, rounded to two decimal places, representing your confidence in the likelihood that the column is a business key component. A value of 0.80 or higher is considered reasonably confident.

            Reason:

            Indicate why you made the decision you did.

            Output:

            Ensure the output conforms to the format shown in the examples below.

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
              "Business Key Component Identification": {
                "Is Business Key Component": true,
                "Confidence Value": 0.95,
                "Reason": "The 'customer_id' column is designated as the primary key, which is typically the best candidate for a business key component in the 'customer' table."
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
              "Business Key Component Identification": {
                "Is Business Key Component": true,
                "Confidence Value": 0.75,
                "Reason": "Although 'id' is not explicitly marked as a primary key, it is likely to uniquely identify each order detail, making it a strong candidate for a business key component."
              }
            }

            Example Input 3)
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

            Column No: 1

            Example Output 3)
            {
              "Business Key Component Identification": {
                "Is Business Key Component": true,
                "Confidence Value": 0.85,
                "Reason": "The 'order_id' column likely represents the primary identifier for each order within the 'order_details' table. Although it is not explicitly marked as a primary key, 'order_id' is a common identifier for business entities, making it a strong candidate for a business key component."
              }
            }

            Example Input 4)
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

            Column No: 2


            Example Output 4)
            {
              "Business Key Component Identification": {
                "Is Business Key Component": true,
                "Confidence Value": 0.80,
                "Reason": "'product_id' likely represents a key component that helps identify specific products associated with the order. It is not the sole key but may serve as part of a composite business key alongside 'order_id'."
              }
            }


            Example Input 5)
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

            Column No: 3

            Example Output 5)
            {
              "Business Key Component Identification": {
                "Is Business Key Component": false,
                "Confidence Value": 0.30,
                "Reason": "The 'quantity' column represents a numeric value related to the number of products in the order, but it does not uniquely identify the record. It is unlikely to serve as a business key component."
              }
            }


            Example Input 6)
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

            Column No: 4

            Example Output 6)
            {
              "Business Key Component Identification": {
                "Is Business Key Component": false,
                "Confidence Value": 0.40,
                "Reason": "'order_date' represents the date on which the order was placed. While it provides important context, it is not unique to the record and is therefore unlikely to serve as a business key component."
              }
            }

            Now, based on the instructions and examples above, please generate the JSON output for the following input. {hints}

            JSON Source Table Object: {new_json}

            Column No: {column_no}
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

            Now, based on the instructions and examples above, please generate the JSON output for the following input. {hints}

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
            - Date or Timestamp (Example: created_date, created_timestamp, update_Date, update_timestamp): Not PII (Unless combined with other identiviers like date of birth)
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

            Now, based on the instructions and examples above, please generate the appropriate JSON output only for the following JSON Source Table Object and Column No inputs.  {hints}

            JSON Source Table Object: {new_json}

            Column No: {column_no}

            "#,
      }
  }
}