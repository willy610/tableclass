use tableclass::explain::explain;
use tableclass::table::Table;
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_orderby::Params2OrderBy;

pub fn ex_orderby(dirpath: &str) {
    //
    // ORDER BY
    //
    let the_sql = "select * from parts order by CITY, COLOR";
    let outer = || -> Result<Table, String> {
        let parts = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?;

        let par_orderby = Params2OrderBy {
            order_cols: Some(vec!["PNO", "COLOR"]),
            ordering_callback: None,
        };
        let result = parts.table_orderby(Some("ORDER BY PNO,COLOR".to_string()), par_orderby);
        result
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_limit' : {}", err);
        }
    }
}
