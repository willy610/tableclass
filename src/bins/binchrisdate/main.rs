use cpu_time::ProcessTime;
use floating_duration::TimeAsFloat;
use std::env;
use tableclass::logit::{logit_disable, logit_enable, logit_reset};

mod chrisdate;
mod ex_distinct;
mod ex_groupby;
mod ex_join;
mod ex_join_test;
mod ex_join_multi;
mod ex_groupby_own_function;
mod ex_where_eq_kind_depsubq;
mod ex_where_eq_kind_once;
mod ex_where_exist_kind_depsub;
mod ex_limit;
mod ex_orderby;
mod ex_project;
mod ex_project_sub;
mod ex_query;
mod ex_where;
mod ex_join_and_project;
mod ex_where_set_kind_once;
mod ex_with_recursive;
mod ex_set;
mod ex_scatter;
mod ex_explain;
/*



*/
use chrisdate::chrisdate;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        panic!("Missing args group testcase");
    }
    let dirpath = &args[1];
    let testscript = &args[2];

    logit_reset(true);
    logit_disable();
    logit_enable(); // comment if no explain trace wanted
    let start = ProcessTime::now();

    chrisdate(dirpath, testscript);
    let dura = start.elapsed();
    eprintln!("Job total cpu-time {} ms", dura.as_fractional_millis());
}
