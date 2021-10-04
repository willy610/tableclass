use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::Params2GroupBy;
use tableclass::table_folder::table_join::Params2Join;
use tableclass::table_folder::table_limit::Params2Limit;
use tableclass::table_folder::table_orderby::Params2OrderBy;

use std::cmp::Ordering;

pub fn popingredients(dirpath: &str) {
    let the_sql = "";

    let outer = || -> Result<Table, String> {
        let ingredient = Table::from_csv(Params2FromCSV {
            table_name: "ingredient",
            dir: dirpath,
            file: "ingredient.csv",
            filter: None,
            project: None,
        });
        let recepiesstepingredient = Table::from_csv(Params2FromCSV {
            table_name: "recepiesstepingredient",
            dir: dirpath,
            file: "recepiesstepingredient.csv",
            filter: None,
            project: None,
        });
        let thejoin = recepiesstepingredient?.table_join(
            Some("ON recepiesstepingredient.ingredientid = ingredient.ingredientid".to_string()),
            Params2Join {
                right_table: &ingredient?,
                joinkindandcols: None,
                cond_left: None,
                cond_right: None,
                cond_both: Some(
                    |left: &Table,
                     right: &Table,
                     leftrow: &TableRow,
                     rightrow: &TableRow|
                     -> Result<bool, String> {
                        let colindex_left = left.colname_2_index("ingredientid")?;
                        let colindex_right = right.colname_2_index("ingredientid")?;
                        Ok(leftrow[colindex_left] == rightrow[colindex_right])
                    },
                ),
                project: None,
            },
        );
        let result = thejoin?.table_groupby(
            Some(
                "SELECT ingredient.ingredientname,COUNT(*) GROUP BY ingredient.ingredientname"
                    .to_string(),
            ),
            Params2GroupBy {
                out_table: "result",
                groupon: Some(vec!["ingredient.ingredientname"]),
                aggrcols: vec![vec!["count", "*", "Used in number of recepies"]],
                custom_aggr: None,
                cond_having: None,
            },
        );
        let result = result?.table_orderby(
            Some("ORDER BY 2 DESC".to_string()),
            Params2OrderBy {
                order_cols: None,
                ordering_callback: Some(|obj: &Table, a: &TableRow, b: &TableRow| -> Ordering {
                    //                        let colindex_recepiestep_aggr_count = 1;
                    let colindex_recepiestep_aggr_count =
                        obj.colname_2_index("Used in number of recepies").unwrap();

                    let left: i32 = a[colindex_recepiestep_aggr_count].parse().unwrap();
                    let right: i32 = b[colindex_recepiestep_aggr_count].parse().unwrap();
                    // order by descending
                    if left < right {
                        Ordering::Greater
                    } else if left > right {
                        Ordering::Less
                    } else {
                        Ordering::Equal
                    }
                }),
            },
        );
        let result = result?.table_limit(
            Some("LIMIT 0,20".to_string()),
            Params2Limit {
                offset: None,
                max_number_rows: Some(20),
            },
        );

        result
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'popingredients' : {}", err);
        }
    }
}
