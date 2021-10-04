use std::f32;
use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::{AggrValues, FuncAndColumn, Params2GroupBy};

pub fn ex_groupby_own_function(dirpath: &str) {
    /*--------------------------------------------------------*/
    // GROUP BY operations
    //
    // One can do aggrgation on groups or without groups
    // For each row there is a callback with an aggragation 'row' (sofar)
    // The following is an example of own function. If you need a mix of standard and
    // copy standard into here they same way as 'min'

    /*--------------------------------------------------------*/
    let the_sql = "select sum(sqrt(WEIGHT)) from P";

    let _outline = Table::table_from_dummy()
        .table_from_outline("FROM CSV parts", "Params")
        .table_groupby_outline("(NO GROUP ON but do calc) sum(sqrt(WEIGHT))", "Params")
        .table_select_outline("sum(sqrt(WEIGHT))", "Params");

    //
    // and in one breath (the group by is big due an own function SUM(SQRT(WEIGHT)) )
    //
    let outer = || -> Result<Table, String> {
        let result = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?
        .table_groupby(
            Some("SELECT SUMSQRROOT(WEIGHT)".to_string()),
            Params2GroupBy {
                out_table: "test_aggr_no_group",
                groupon: None,
                aggrcols: vec![vec!["SUMSQRROOT", "WEIGHT", "SUMSQRROOT(WEIGHT)"]],
                custom_aggr: Some(
                    |obj: &Table,
                     row: &TableRow,
                     in_aggr_value_sofar: &AggrValues|
                     -> Result<AggrValues, String> {
                        let mut aggr_value_sofar = in_aggr_value_sofar.clone();

                        let my_funtions:Result<Vec<FuncAndColumn>,String> = vec![
                            vec!["SUMSQRROOT", "WEIGHT"],
                            //vec!["min", "WEIGHT"], // not used
                        ]
                        .iter()
                        .enumerate()
                        .map(|(i, function_and_colname)|->Result<FuncAndColumn,String> {
                            let func = function_and_colname[0].to_string();
                            let colname = function_and_colname[1];
                            Ok(FuncAndColumn {
                                a_func: func,
                                index_in_aggr: i,
                                index_in_row: if colname == "*" {
                                    usize::MAX
                                } else {
                                    obj.colname_2_index(colname)?
                                },
                            })
                        })
//                        .collect::<Vec<FuncAndColumn>>();
                        .collect();

                        if aggr_value_sofar.first_call {
                            aggr_value_sofar.first_call = false;
                            aggr_value_sofar.aggr.cnt = 1;
                            for x in my_funtions? {
                                if x.index_in_row == usize::MAX {
                                    // is count(*)
                                    aggr_value_sofar.aggr.aggr_values[x.index_in_aggr] =
                                        "1".to_string();
                                } else {
                                    aggr_value_sofar.aggr.aggr_values[x.index_in_aggr] =
                                        row[x.index_in_row].clone();
                                }
                            }
                        } else {
                            aggr_value_sofar.aggr.cnt = aggr_value_sofar.aggr.cnt + 1;
                            for x in my_funtions? {
                                match x.a_func.as_str() {
                                    /*
                                    "min" => {
                                        let old_min: f32 =
                                            aggr_value_sofar.aggr.aggr_values[x.index_in_aggr].parse().unwrap();
                                        let this_row_value: f32 = row[x.index_in_row].parse().unwrap();
                                        if this_row_value < old_min {
                                            aggr_value_sofar.aggr.aggr_values[x.index_in_aggr] =
                                                this_row_value.to_string();
                                        }
                                    }
                                    */
                                    "SUMSQRROOT" => {
                                        let old_sum: f32 = aggr_value_sofar.aggr.aggr_values
                                            [x.index_in_aggr]
                                            .parse()
                                            .unwrap();
                                        let this_row_value: f32 =
                                            row[x.index_in_row].parse::<f32>().unwrap().sqrt();
                                        aggr_value_sofar.aggr.aggr_values[x.index_in_aggr] =
                                            (old_sum + this_row_value).to_string();
                                    }
                                    _ => {
                                        return Err(format!(
                                            "In group by the aggregate function '{}' not supported",
                                            x.a_func
                                        ))
                                    }
                                }
                            }
                        }
                        Ok(aggr_value_sofar.clone())
                    },
                ),
                cond_having: None,
            },
        );
        result
        //    eprintln!("{}\n{}", the_sql, result.show());
        //    explain(&result, the_sql);
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_distinct' : {}", err);
        }
    }
}
