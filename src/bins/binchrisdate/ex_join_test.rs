use tableclass::explain::explain;
use tableclass::table::Table;
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_join::{JoinKindAndCols, Params2Join};

pub fn ex_join_test(dirpath: &str) {
    let the_sql = "SELECT * FROM P JOIN S ON S.CITY = P.CITY";

    let outer = || -> Result<Table, String> {
        let parts = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?;
        let supplier = Table::from_csv(Params2FromCSV {
            table_name: "supplier",
            dir: dirpath,
            file: "supplier.csv",
            filter: None,
            project: None,
        })?;
        let result_table = parts.table_join(
            Some("ON S.CITY = P.CITY".to_string()),
            Params2Join {
                right_table: &supplier,
                joinkindandcols: Some(JoinKindAndCols {
                    joinhow: "OUTER",
                    joincols: vec![vec!["CITY", "CITY"]],
                }),
                cond_left: None,
                cond_right: None,
                cond_both: None,
                project: None,
            },
        );
        //    eprintln!("{}", result_table.show());
        //    explain(&result_table, the_sql)
        result_table
    };
    match outer() {
        Ok(result_set) => {
            //            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_distinct' : {}", err);
        }
    }
}
