use tableclass::explain::explain;
use tableclass::table::Table;
use tableclass::table_folder::table_from::Params2FromCSV;

use tableclass::table_folder::table_limit::Params2Limit;

pub fn ex_limit(dirpath: &str) {
    // LIMIT start,limit
    //    select distinct CITY from P
    //LIMIT [offset,] row_count
    let the_sql = "SELECT * from P LIMIT 1,4";
    let outer = || -> Result<Table, String> {
        let parts = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: None,
            project: None,
        })?;

        let par = Params2Limit {
            offset: Some(1),
            max_number_rows: Some(4),
        };

        let parts = parts.table_limit(Some("limit 10,60".to_string()), par);
        //    eprintln!("{}\n{}", the_sql, parts.show());
        //    explain(&parts, the_sql);
        /*
        let result1 = parts.table_limit(Some("limit 1,2".to_string()), Some(2), Some(2));
        eprintln!("select * from P limit 2,2 \n{}", result1.show());

        let result1 = parts.table_limit(Some("limit 3".to_string()), Some(3), None);
        eprintln!("select * from P limit 3\n{}", result1.show());

        let result1 = parts.table_limit(None, None, None);
        eprintln!("select * from P\n{}", result1.show());
        */
        parts
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
