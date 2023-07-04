use std::env;
use cucumber::{gherkin::Step, given, then, when};
use futures::TryStreamExt;
use typedb_client::{answer::Numeric, Result as TypeDBResult};
use typeql_lang::parse_query;

use crate::{
    behaviour::{parameter::LabelParam, util, Context},
    generic_step_impl
};

generic_step_impl! {
#[step(expr = "set time-zone is: {word}")]
async fn set_time_zone(context: &mut Context, timezone: String) {
    env::set_var("TZ", timezone);
}
}