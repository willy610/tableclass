use cpu_time::ProcessTime;
use floating_duration::TimeAsFloat;
use std::cmp::Ordering;
use std::env;
use tableclass::explain::explain;
use tableclass::logit::{logit_disable, logit_enable, logit_reset};
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::Params2GroupBy;
use tableclass::table_folder::table_limit::Params2Limit;
use tableclass::table_folder::table_orderby::Params2OrderBy;

pub fn main() {
    logit_reset(true);
    logit_disable();
    logit_enable(); /* comment if no log and explain trace required */
    // ------------------------
    // Real SQL
    //
    let the_sql = 
"SELECT recid, Sum(minutes)
 FROM recepistep 
 GROUP  BY recid
 ORDER  BY Sum(minutes) DESC
 LIMIT 10";
    // ------------------------
    // Outline here
    //
    {// block will be released
        let _outline = Table::table_from_dummy()
            .table_from_outline("FROM CSV recepiestep", "Params")
            .table_groupby_outline("GROUP BY recid and aggr->Sum(minutes)", "Params")
            .table_orderby_outline("ORDER BY Sum(minutes) DESC", "Params")
            .table_limit_outline("LIMIT 30", "Params")
            .table_select_outline("SELECT recid, Sum(minutes)", "Params");

//        eprintln!("{}", _outline.show());
    }
    //
    // ------------------------
    // And 'decorate' the outline with parameters into real values
    //
    let start = ProcessTime::now();
    {
        // block will be released after process. 
        // it will take time. 
        // so at the end the duration is more fair
        let outer = || -> Result<Table, String> {
            let bind_limit = Some(10);
            let res = Table::from_csv(Params2FromCSV {
                table_name: "recepiestep",
                dir: "./src/bins/binrecepie/data",
                file: "recepiestep.csv",
                filter: None,
                project: None,
            })?
            .table_groupby(
                Some("GROUP BY recid".to_string()),
                Params2GroupBy {
                    out_table: "groupedon",
                    groupon: Some(vec!["recid"]),
                    aggrcols: vec![vec!["SUM", "minutes", "Sum(minutes)"]],
                    custom_aggr: None,
                    cond_having: None,
                },
            )?
            .table_orderby(
                Some("ORDER BY Sum(minutes) DESC".to_string()),
                Params2OrderBy {
                    order_cols: None,
                    ordering_callback: Some(
                        |obj: &Table, a: &TableRow, b: &TableRow| -> Ordering {
                            let colindex_recepiestep_aggr_sum =
                                obj.colname_2_index("Sum(minutes)").unwrap();
                            let left: i32 = a[colindex_recepiestep_aggr_sum].parse().unwrap();
                            let right: i32 = b[colindex_recepiestep_aggr_sum].parse().unwrap();
                            // order by descending
                            if left < right {
                                Ordering::Greater
                            } else if left > right {
                                Ordering::Less
                            } else {
                                Ordering::Equal
                            }
                        },
                    ),
                },
            )?
            .table_limit(
                Some("LIMIT 10".to_string()),
                Params2Limit {
                    offset: None,
                    max_number_rows: bind_limit,
                    // or  max_number_rows: Some(10)
                },
            );
            res
        };
        match outer() {
            Ok(result_set) => {
                let args: Vec<String> = env::args().collect();

                // result_set, which is as a Table-obj, is now computational with plain RUST

                if args.len() == 2 {
                    match args[1].as_str() {
                        "--tty" => {
                            println!("{}", result_set.show())
                        }
                        "--svg" => explain(&result_set, the_sql),
                        "--csv" => {
                            println!("{}", result_set.table_as_csv())
                        }
                        _ => {}
                    }
                }
            }
            Err(err) => {
                eprintln!("Some error in 'ex_distinct' : {}", err);
            }
        }
    }
    // most memory is released. that might take some time. fair to collect
    let dura = start.elapsed();
    eprintln!("Job total cpu-time {} ms", dura.as_fractional_millis());
}
