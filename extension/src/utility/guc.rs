use pgrx::prelude::*;
use pgrx::guc::*;
use std::ffi::CStr;

use anyhow::Result;

pub static PG_AUTO_DW_DATABASE_NAME: GucSetting<Option<&CStr>> = GucSetting::<Option<&CStr>>::new(None);

pub fn init_guc() {
    // Register the GUCs
    GucRegistry::define_string_guc(
        "pg_auto_dw.database_name",
        "The database name for the extension.",
        "This is the database name used by the extension.",
        &PG_AUTO_DW_DATABASE_NAME,
        GucContext::Userset,
        GucFlags::default(),
    );
}

// for handling of GUCs that can be error prone
#[derive(Clone, Debug)]
pub enum PgAutoDWGuc {
    DatabaseName,
}

/// a convenience function to get this project's GUCs
pub fn get_guc(guc: PgAutoDWGuc) -> Option<String> {

    let val = match guc {
        PgAutoDWGuc::DatabaseName => PG_AUTO_DW_DATABASE_NAME.get(),
    };

    if let Some(cstr) = val {
        if let Ok(s) = handle_cstr(cstr) {
            Some(s)
        } else {
            error!("failed to convert CStr to str");
        }
    } else {
        info!("no value set for GUC: {:?}", guc);
        None
    }
}

#[allow(dead_code)]
fn handle_cstr(cstr: &CStr) -> Result<String> {
    if let Ok(s) = cstr.to_str() {
        Ok(s.to_owned())
    } else {
        Err(anyhow::anyhow!("failed to convert CStr to str"))
    }
}