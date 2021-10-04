use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_where::{Params2Where, SimpleOrComplex};

pub fn ex_where(dirpath: &str) {
    /*--------------------------------------------------------*/
    //
    // WHERE
    //
    //    SELECT * FROM P WHERE CITY = 'London'
    //
    let the_sql = "SELECT * FROM P WHERE CITY = 'London'";
    let outer = || -> Result<Table, String> {
        let result = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?
        .table_where(
            Some("WHERE CITY = 'London'".to_string()),
            Params2Where {
                super_obj: None,
                super_row: None,
                simple_or_complex: SimpleOrComplex::SimpleCond(
                    |obj: &Table,
                     row: &TableRow,
                     _superobj: Option<&Table>,
                     _superrow: Option<TableRow>|
                     -> Result<bool, String> {
                        let colindex_city = obj.colname_2_index("CITY")?;
                        Ok(row[colindex_city] == "London")
                    },
                ),
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
            eprintln!("Some error in 'ex_where' : {}", err);
        }
    }
}
