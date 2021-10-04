use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_select::Params2Select;
use tableclass::table_folder::table_where::{
    OnceOrDepSub, Params2Where, SimpleOrComplex, SubOrOnceAndCheck,
};

/*--------------------------------------------------------*/
/*
 > ALL ... (independent subquery)


SELECT *
FROM P AS PX
WHERE PX.weight > ALL
    (SELECT PY.weight
     FROM P AS PY
     WHERE PY.COLOR = 'Blue')
*/
/*--------------------------------------------------------------------------------*/

pub fn ex_where_set_kind_once(dirpath: &str) {
    //
    // DERIVED TABLE AS SUBQUERY. ONCE INVOKATION OF SUBQ
    //
    let the_sql = "SELECT *
    FROM P AS PX
    WHERE PX.weight > ALL
        (SELECT PY.weight
         FROM P AS PY
         WHERE PY.COLOR = 'Blue')";
         let outer = || -> Result<Table, String> {

    let subq_once = Table::from_csv(Params2FromCSV {
        table_name: "part",
        dir: dirpath,
        file: "part.csv",
        filter: None,
        project: None,
    })?
    .table_where(
        Some("SELECT PY.weight FROM P AS PY WHERE PY.COLOR = 'Blue'".to_string()),
        Params2Where {
            super_obj: None,
            super_row: None,
            simple_or_complex: SimpleOrComplex::SimpleCond(
                |subobj: &Table,
                 subrow: &TableRow,
                 _superobj: Option<&Table>,
                 _superrow: Option<TableRow>|
                 -> Result<bool, String> {
                    let colindex_sub_color = subobj.colname_2_index("COLOR")?;
                    return Ok(subrow[colindex_sub_color] == "Blue".to_string());
                },
            ),
        },
    )?
    .table_select(
        Some("SELECT PY.weight".to_string()),
        Params2Select {
            from_cols: vec!["WEIGHT"],
            to_table_name: None,
            to_column_names: None,
        },
    );

    let par_where = Params2Where {
        super_obj: None,
        super_row: None,

        simple_or_complex: SimpleOrComplex::ComplexCond(SubOrOnceAndCheck {
            once_or_sub: OnceOrDepSub::OnceSub(subq_once?),
            the_eval_subq_result_function: |superobj: &Table,
                                            superrow: &TableRow,
                                            thesubqoutcome: &Table|
             -> Result<bool, String> {
                let colindex_super_weigth = superobj.colname_2_index("WEIGHT")?;
                let colindex_sub_weigth = thesubqoutcome.colname_2_index("WEIGHT")?;
                let how = thesubqoutcome
                    .rows
                    .iter()
                    // Test for PX.weight > ALL (...)
                    // oper is '>= ANY' like in ...supervaris >= ALL (select ...)
                    .all(|subrow| subrow[colindex_sub_weigth] > superrow[colindex_super_weigth]);
                return Ok(how && thesubqoutcome.rows.len() > 0) // empty collections will return TRUE
            },
        }),
    };
    //
    // START HERE
    //

    let parts = Table::from_csv(Params2FromCSV {
        table_name: "part",
        dir: dirpath,
        file: "part.csv",
        filter: None,
        project: None,
    })?;

    let result = parts.table_where(Some("WHERE PX.weight > ALL".to_string()), par_where);
    result
     };
     match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_where_set_kind_once' : {}", err);
        }
    }
}
