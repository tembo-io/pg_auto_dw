use crate::model::prompt_template::PromptTemplate;
use super::{guc, openai_client, ollama_client};
use TransformerServerType::{OpenAI, Ollama};
use std::str::FromStr;

pub enum TransformerServerType {
    OpenAI,
    Ollama
}

impl FromStr for TransformerServerType {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<TransformerServerType, Self::Err> {
        match s.to_lowercase().as_str() {
            "openai" => Ok(OpenAI),
            "ollama" => Ok(Ollama),
            _ => Err("Invalid Transformer Server Type"),
        }
    }
}

pub async fn send_request(new_json: &str, template_type: PromptTemplate, col: &u32, hints: &str)  -> Result<serde_json::Value, Box<dyn std::error::Error>>  {
    
        let transformer_server_type_str = guc::get_guc(guc::PgAutoDWGuc::TransformerServerType).ok_or("GUC: Transformer Server Type is not set.")?;

        let transformer_server_type = transformer_server_type_str.parse::<TransformerServerType>()
            .map_err(|e| format!("Error parsing Transformer Server Type: {}", e))?;

    match transformer_server_type {
        OpenAI => openai_client::send_request(new_json, template_type, col, hints).await,
        Ollama => ollama_client::send_request(new_json, template_type, col, hints).await,
    }
}

