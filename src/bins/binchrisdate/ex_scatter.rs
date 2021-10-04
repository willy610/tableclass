use std::cmp::Ordering;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::Params2GroupBy;
use tableclass::table_folder::table_limit::Params2Limit;
use tableclass::table_folder::table_orderby::Params2OrderBy;
use tableclass::table_folder::table_scatter::Params2Scatter;

pub fn ex_scatter(dirpath: &str) {
    let the_sql = "SELECT SNO,PNO,MAX(QTY) FROM SP GROUP BY SNO,PNO ORDER BY 3 DESC LIMIT 0,10";
    let outer = || -> Result<Table, String> {
        let supplierpart = Table::from_csv(Params2FromCSV {
            table_name: "supplierpart",
            dir: dirpath,
            file: "supplierpart.csv",
            filter: None,
            project: None, // Pick desired column here in order to avoid the next step!
        })?;

        let result = supplierpart
            .table_groupby(
                Some("GROUP BY SNO,PNO".to_string()),
                Params2GroupBy {
                    out_table: "result",
                    groupon: Some(vec!["SNO", "PNO"]),
                    aggrcols: vec![vec!["max", "QTY", "Max Quantity"]],
                    custom_aggr: None,
                    cond_having: None,
                },
            )?
            .table_orderby(
                Some("ORDER BY 3 DESC".to_string()),
                Params2OrderBy {
                    order_cols: None,
                    ordering_callback: Some(
                        |obj: &Table, a: &TableRow, b: &TableRow| -> Ordering {
                            let colindex_recepiestep_aggr_max =
                                obj.colname_2_index("Max Quantity").unwrap();

                            let left: i32 = a[colindex_recepiestep_aggr_max].parse().unwrap();
                            let right: i32 = b[colindex_recepiestep_aggr_max].parse().unwrap();
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
                Some("limit 0,10".to_string()),
                Params2Limit {
                    offset: Some(0),
                    max_number_rows: Some(10),
                },
            );
        result
    };
    /*
    eprintln!("{}\n{}", the_sql, result.show());
    */
    match outer() {
        Ok(result_set) => {
            //            eprintln!("{}", result_set.show());
            eprintln!(
                "{}\n{}",
                the_sql,
                result_set.table_scatter(Params2Scatter {
                    row_y_from_column: "SNO",
                    column_x_from_column: "PNO",
                    value_from_column: "Max Quantity"
                })
            );
        }
        Err(err) => {
            eprintln!("Some error in 'ex_scatter' : {}", err);
        }
    }
}
