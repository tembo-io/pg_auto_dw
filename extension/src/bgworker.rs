use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;
use std::thread::sleep;

use crate::queries;

#[pg_guard]
pub extern "C" fn _PG_init() {
    BackgroundWorkerBuilder::new("Background Worker Example")
        .set_function("background_worker_main")
        .set_library("pg_auto_dw")
        .set_argument(42i32.into_datum())
        .enable_spi_access()
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn background_worker_main(arg: pg_sys::Datum) {
    let arg = unsafe { i32::from_polymorphic_datum(arg, false, pg_sys::INT4OID) };

    // these are the signals we want to receive.  If we don't attach the SIGTERM handler, then
    // we'll never be able to exit via an external notification
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // we want to be able to use SPI against the specified database (postgres), as the superuser which
    // did the initdb. You can specify a specific user with Some("my_user")
    // Q: Does this default to pg_auto_dw
    BackgroundWorker::connect_worker_to_spi(Some("pg_auto_dw"), None);

    log!(
        "Hello from inside the {} BGWorker!  Argument value={}",
        BackgroundWorker::get_name(),
        arg.unwrap()
    );
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        let result: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
            Spi::connect(|_| {
                Spi::run("SELECT 'Hello'")?;
                // Spi::run(queries::SOURCE_OBJECTS_UPDATE)?;
                // Spi::run(queries::source_object_dw( 
                //     "a^", 
                //     "a^", 
                //     "a^", 
                //     "a^", 
                //     "a^", 
                //     "a^")
                //     .as_str())?;
                // log!("Client BG Worker");
                Ok(())
            })
        });
        result.unwrap_or_else(|e| panic!("got an error: {}", e));
}


    log!("Goodbye from inside the {} BGWorker! ", BackgroundWorker::get_name());
}