use cpu_time::ProcessTime;
use floating_duration::TimeAsFloat;
use std::env;

use tableclass::logit::{logit_disable, logit_enable, logit_reset};

pub mod recepies;
pub mod popingredients;
pub mod complexrecepies;

use crate::recepies::recepies;

pub fn main() {

// ./target/release/binrecepie ./src/bins/binrecepie/data popingredients >./src/bins/binrecepie/data/svgs/popingredients.svg 
// ./target/release/binrecepie ./src/bins/binrecepie/data complexrecepies >./src/bins/binrecepie/data/svgs/complexrecepies.svg 

let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Missing args group testcase");
    }
    let dirpath = &args[1];
    let testscript = &args[2];

    logit_reset(true);
    logit_disable();
    logit_enable(); // comment if no explain trace wanted
    let start = ProcessTime::now();
    recepies(dirpath, testscript);
    let dura = start.elapsed();
    eprintln!("Job total cpu-time {} ms", dura.as_fractional_millis());
}
