use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

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
    BackgroundWorker::connect_worker_to_spi(Some("postgres"), None);

    log!(
        "Hello from inside the {} BGWorker!  Argument value={}",
        BackgroundWorker::get_name(),
        arg.unwrap()
    );
    // wake up every 10s or if we received a SIGTERM
    while BackgroundWorker::wait_latch(Some(Duration::from_secs(10))) {
        if BackgroundWorker::sighup_received() {
            // on SIGHUP, you might want to reload some external configuration or something
            log!("SIGHUP received: Reloading configuration...");
        }

        // within a transaction, execute an SQL statement, and log its results
        let result: Result<(), pgrx::spi::Error> = BackgroundWorker::transaction(|| {
            Spi::connect(|client| {
                let tuple_table = client.select(
                    "SELECT 'Hi', id, ''||a FROM (SELECT id, 42 from generate_series(1,10) id) a ",
                    None,
                    None,
                )?;
                for tuple in tuple_table {
                    let a = tuple.get_datum_by_ordinal(1)?.value::<String>()?;
                    let b = tuple.get_datum_by_ordinal(2)?.value::<i32>()?;
                    let c = tuple.get_datum_by_ordinal(3)?.value::<String>()?;
                    // log!("from bgworker: ({:?}, {:?}, {:?})", a, b, c);
                }
                Ok(())
            })
        });
        result.unwrap_or_else(|e| panic!("got an error: {}", e))
    }

    log!("Goodbye from inside the {} BGWorker! ", BackgroundWorker::get_name());
}