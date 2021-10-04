use tableclass::explain::explain;
use tableclass::table::{Table, TableRow};
use tableclass::table_folder::table_from::Params2FromCSV;
use tableclass::table_folder::table_groupby::Params2GroupBy;
use tableclass::table_folder::table_join::Params2Join;
use tableclass::table_folder::table_orderby::Params2OrderBy;
use tableclass::table_folder::table_select::Params2Select;
use tableclass::table_folder::table_where::{Params2Where, SimpleOrComplex};

pub fn ex_explain(dirpath: &str) {
    //
    // A stupid example but with many constructions
    //
    let the_sql = "SELECT PNO
	,pCITY
	,MAX(spQTY)
	,COUNT(*)
FROM (
	SELECT REDUCEDP.PNO
		,REDUCEDP.CITY
		,SP.SNO
		,SP.QTY
	FROM (
		SELECT PNO
			,PNAME
			,WEIGHT
			,CITY
		FROM P
		WHERE WEIGHT > 14
		) AS REDUCEDP
	JOIN SP ON REDUCEDP.PNO = SP.PNO
	) AS projett(PNO, pCITY, SNO, spQTY)
WHERE pCITY = 'London'
	OR pCITY = 'Paris'
GROUP BY PNO
	,pCITY
ORDER BY PNO
	,pCITY";

    let outline = Table::table_from_dummy()
        .table_from_outline("FROM CSV parts", "Params")
        .table_where_outline("WHERE WEIGHT > 14", "Params filter..")
        .table_select_outline("PNO, PNAME, WEIGHT,CITY", "Params pick certain columns ")
        .table_join_outline("supplierpart ON P.PNO = SP.PNO", "Params")
        .table_select_outline(
            "parts.PNO, parts.CITY, supplierpart.SNO, supplierpart.QTY 
            AS projett (PNO,pCITY,SNO,spQTY)",
            "Params",
        )
        .table_where_outline("WHERE pCITY = 'London' or pCITY ='Paris'", "Params")
        .table_groupby_outline("SELECT MAX(spQTY),COUNT(*) GROUP BY PNO,pCITY", "Params")
        .table_orderby_outline("ORDER BY PNO,pCITY", "Params");

    eprintln!("{}", outline.show());
    let outer = || -> Result<Table, String> {
        let result = Table::from_csv(Params2FromCSV {
            table_name: "parts",
            dir: dirpath,
            file: "part.csv",
            filter: Some(|headers, arow| -> bool {
                arow[headers
                    .iter()
                    .position(|r| r == &"WEIGHT".to_string())
                    .unwrap()]
                .to_string()
                .parse::<f32>()
                .unwrap()
                    > 14.0
            }),
            project: Some(vec!["PNO", "PNAME", "WEIGHT", "CITY"]),
        })?
        .table_join(
            Some("ON parts.PNO = supplierpart.PNO".to_string()),
            Params2Join {
                right_table: &Table::from_csv(Params2FromCSV {
                    table_name: "supplierpart",
                    dir: dirpath,
                    file: "supplierpart.csv",
                    filter: None,
                    project: None,
                })?,
                joinkindandcols: None,
                cond_left: None,
                cond_right: None,
                cond_both: Some(
                    |left: &Table,
                     right: &Table,
                     leftrow: &TableRow,
                     rightrow: &TableRow|
                     -> Result<bool,String> {
                        let colindex_left = left.colname_2_index("PNO")?;
                        let colindex_right = right.colname_2_index("PNO")?;
                        Ok(leftrow[colindex_left] == rightrow[colindex_right])
                    },
                ),
                project: None,
            },
        )?
        .table_select(
            Some(
                "SELECT parts.PNO, 
        parts.CITY, 
        supplierpart.SNO, 
        supplierpart.QTY AS projett (PNO,pCITY,SNO,spQTY)"
                    .to_string(),
            ),
            Params2Select {
                from_cols: vec![
                    "parts.PNO",
                    "parts.CITY",
                    "supplierpart.SNO",
                    "supplierpart.QTY",
                ],
                to_table_name: Some("projett"),
                to_column_names: Some(vec!["PNO", "pCITY", "SNO", "spQTY"]),
            },
        )?
        .table_where(
            Some("WHERE pCITY = 'London' or pCITY ='Paris'".to_string()),
            Params2Where {
                super_obj: None,
                super_row: None,
                simple_or_complex: SimpleOrComplex::SimpleCond(
                    |obj: &Table,
                     row: &TableRow,
                     _super_obj: Option<&Table>,
                     _super_row: Option<TableRow>|
                     -> Result<bool,String> {
                        let colindex_city = obj.colname_2_index("pCITY")?;
                        Ok(row[colindex_city] == "London" || row[colindex_city] == "Paris")
                    },
                ),
            },
        )?
        .table_groupby(
            Some("SELECT MAX(spQTY),COUNT(*) GROUP BY PNO,pCITY".to_string()),
            Params2GroupBy {
                out_table: "aftergroupby",
                groupon: Some(vec!["PNO", "pCITY"]),
                aggrcols: vec![
                    vec!["max", "spQTY", "MAX(spQTY)"],
                    vec!["count", "*", "COUNT(*)"],
                ],
                custom_aggr: None,
                cond_having: None,
            },
        )?
        .table_orderby(
            Some("ORDER BY PNO,pCITY".to_string()),
            Params2OrderBy {
                order_cols: Some(vec!["PNO", "pCITY"]),
                ordering_callback: None,
            },
        );
        result
    };
    //    eprintln!("{}", result.show());
    //    explain(&result, the_sql);
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, the_sql);
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_set' : {}", err);
        }
    }
}
