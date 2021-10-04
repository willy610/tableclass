use tableclass::explain::explain;
use tableclass::table::Table;
use tableclass::table_folder::table_from::Params2FromCSV;

use tableclass::table_folder::table_select::Params2Select;

pub fn ex_project(dirpath: &str) {
    // PROJECT
    //    select PNO as pno,CITY as city from ...
    //
    let the_sql = "SELECT PNO AS pno,CITY as city FROM P";
    let outer = || -> Result<Table, String> {
        let parts = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?;
        let result = parts.table_select(
            Some("SELECT PNO AS pno, CITY AS city".to_string()),
            Params2Select {
                from_cols: vec!["PNO", "CITY"],
                to_table_name: None,
                to_column_names: Some(vec!["pno", "city"]),
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
            eprintln!("Some error in 'ex_project' : {}", err);
        }
    }
}
