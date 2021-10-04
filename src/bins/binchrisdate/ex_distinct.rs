use tableclass::explain::explain;
use tableclass::table::Table;

use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_select::Params2Select;
pub fn ex_distinct(dirpath: &str) {
    //
    // DISTINCT
    //
    let the_sql = "SELECT DISTINCT CITY FROM parts";

    let outer = || -> Result<Table, String> {
        // ------------------------
        // Outline here
        //
        let outline = Table::table_from_dummy()
            .table_from_outline("FROM CSV parts", "Params")
            .table_select_outline("SELECT DISTINCT CITY", "Params");

        eprintln!("{}", outline.show());

        // and implement

        let result = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?
        .table_select(
            Some("SELECT CITY".to_string()),
            Params2Select {
                from_cols: vec!["CITY"],
                to_table_name: None,
                to_column_names: Some(vec!["CITY"]),
            },
        )?
        .table_distinct(Some("DISTINCT CITY".to_string()));
        // Show intermediate result
        eprintln!("{}",(result.clone().unwrap()).show());
        result
    };
    match  outer(){
        Ok(result_set)=>{
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational 
        }
        Err(err)=>{eprintln!("Some error in 'ex_distinct' : {}", err);}
    }
}
