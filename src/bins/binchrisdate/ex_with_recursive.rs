use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;

use tableclass::table_folder::table_where::{Params2Where, SimpleOrComplex};
use tableclass::table_folder::table_with_recursive::Params2WithRecursive;

pub fn ex_with_recursive(dirpath: &str) {
    /*
    Display an employee and its bosses all the way up the the BOSS
    */
    /*
    WITH recursive
    supern (personx,bossx,weightx,agex)
    AS
      (
             SELECT person,
                    boss,
                    weight,
                    age
             FROM   persons
             WHERE  Person='Davis'
             UNION ALL
             SELECT person,
                    boss,
                    weight,
                    age
             FROM   persons AS subben,
                    supern
             WHERE  subben.person = supern.bossx )
      SELECT *
      FROM   supern
          */

    // First create the final table with one initial row value
    //    let the_sql = "SELECT Person,Boss,Weight,Age FROM Ledger WHERE Person = 'Davis'";
    let the_sql = "WITH recursive 
supern (personx,bossx,weightx,agex) 
AS 
  ( 
         SELECT person, 
                boss, 
                weight, 
                age 
         FROM   persons 
         WHERE  Person='Davis' 
         UNION ALL 
         SELECT person, 
                boss, 
                weight, 
                age 
         FROM   persons AS subben, 
                supern 
         WHERE  subben.person = supern.bossx ) 
  SELECT * 
  FROM   supern

";
    let outer = || -> Result<Table, String> {
        let ledger = Table::from_csv(Params2FromCSV {
            table_name: "Ledger",
            dir: dirpath,
            file: "Ledger.csv",
            filter: None,
            project: None,
        })?;
        //
        // Pick up the start row
        //
        let result = ledger.table_where(
            Some("WHERE Person = 'Davis'".to_string()),
            Params2Where {
                super_obj: None,
                super_row: None,
                simple_or_complex: SimpleOrComplex::SimpleCond(
                    |obj: &Table,
                     row: &TableRow,
                     _superobj: Option<&Table>,
                     _superrow: Option<TableRow>|
                     -> Result<bool, String> {
                        let colindex_person = obj.colname_2_index("Person")?;
                        Ok(row[colindex_person] == "Davis")
                    },
                ),
            },
        )?;
        eprintln!("{}\n{}", the_sql, result.show());
        if result.rows.len() == 0 {
            return Err("ex_cte_sub(). Must have some row to start with ".to_string());
        }

        let my_dep_sub = |superobj: &Table,
                          superrow: &TableRow,
                          tables: &Vec<&Table>|
         -> Result<Table, String> {
            tables[0].table_where(
                Some("WHERE superobj.Boss = obj.Person".to_string()),
                Params2Where {
                    super_obj: Some(superobj),
                    super_row: Some(superrow.to_vec()),
                    simple_or_complex: SimpleOrComplex::SimpleCond(
                        //    superobj.Person = obj.Boss
                        |obj: &Table,
                         row: &TableRow,
                         superobj: Option<&Table>,
                         superrow: Option<TableRow>|
                         -> Result<bool, String> {
                            match superobj {
                                Some(superobj) => match superrow {
                                    Some(superrow) => Ok(superrow
                                        [superobj.colname_2_index("Boss")?]
                                        == row[obj.colname_2_index("Person")?]),
                                    None => Err("superrow missing".to_string()),
                                },
                                None => Err("superobj missing".to_string()),
                            }
                        },
                    ),
                },
            )
        };
        let a_cte = result.table_with_recursive(
            Some("WHERE  subben.person = supern.bossx".to_string()),
            Params2WithRecursive {
                depsubq: my_dep_sub,
                extra_tables: &vec![&ledger],
            },
        );
        a_cte
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_with_recursive' : {}", err);
        }
    }
}
