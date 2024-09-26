use crate::model::prompt_template::PromptTemplate;
use super::{guc, openai_client, ollama_client};
use TransformerServerType::{OpenAI, Ollama};

pub enum TransformerServerType {
    OpenAI,
    Ollama
}

pub async fn send_request(new_json: &str, template_type: PromptTemplate, col: &u32, hints: &str) 
    -> Result<serde_json::Value, Box<dyn std::error::Error>>  {
    
        let transformer_server_type = guc::get_guc(guc::PgAutoDWGuc::TransformerServerType).ok_or("GUC: Transformer Server Type is not set.")?;
        let transformer_server_type = {
            if transformer_server_type == "openai".to_string() {
                TransformerServerType::OpenAI
            } else {
                TransformerServerType::Ollama
            }
        };

    match transformer_server_type {
        OpenAI => {openai_client::send_request(new_json, template_type, col, hints).await},
        Ollama => {ollama_client::send_request(new_json, template_type, col, hints).await},
    }
    
}

