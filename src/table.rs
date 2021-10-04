extern crate csv;
use std::time::Duration;
pub type TableRow = Vec<String>;
pub type TableRows = Vec<TableRow>;
//////////////////////////////////////////////////////////////////
//  for subqueries
//
pub type SubQueryFunction = fn(&Table, &TableRow, &Vec<&Table>) -> Result<Table, String>;
//
/////////////////////////////////////////////////

// for OperLog
//////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub enum SourceOrDerived {
    Source,
    Derived,
}
#[derive(Clone, Debug)]
pub struct LogSourceType {
    pub intable_id: Option<i32>,
    pub intable_name: String,
    pub sor_or_deriv: SourceOrDerived,
}
//https://stackoverflow.com/questions/32710187/how-do-i-get-an-enum-as-a-string
#[derive(Clone, Debug)]
pub enum LogOper {
    Distinct,
    FromCSV,
    FromStr,
    GroupBy,
    Join,
    Limit,
    OrderBy,
    SelectWithSub,
    Select,
    Union,
    Intersection,
    Except,
    Where,
    WhereDepSub,
    WithRecursive,
}
use std::fmt::{self, Debug};
impl fmt::Display for LogOper {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
#[derive(Clone, Debug)]
pub struct OperLog {
    pub outtablename: String,
    pub operation: LogOper,
    pub duration: Duration,
    pub nrrows: usize,
    pub nrcols: usize,
    pub out_table_id: i32,
    pub sqlstm: Option<String>,
    pub legs: Vec<LogSourceType>,
}
impl OperLog {
    pub fn new(
        operation: LogOper,
        outtablename: String,
        duration: Duration,
        nrrows: usize,
        nrcols: usize,
        out_table_id: i32,
        sqlstm: Option<String>,
        legs: Vec<LogSourceType>,
    ) -> OperLog {
        OperLog {
            operation: operation,
            duration: duration,
            outtablename: outtablename,
            nrrows: nrrows,
            nrcols: nrcols,
            out_table_id: out_table_id,
            sqlstm: sqlstm,
            legs: legs,
        }
    }
}
// Table itself
//////////////////////////////////////////////////////////////////
#[derive(Clone, Debug)]
pub struct Table {
    pub table_name: String,
    pub headers: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub id: i32,
}
impl Table {
    pub fn new(table_name: &str, headers: Vec<String>, rows: TableRows, id: i32) -> Table {
        Table {
            table_name: table_name.to_string(),
            headers: headers,
            rows: rows,
            id: id,
        }
    }
    //--------------------------------
    /*--------------------------------------------------------*/
    pub fn table_select_sub_outline(&self, _sqlstm: Option<&str>, _par: Option<&str>) -> Table {
        self.clone()
    }
    /*--------------------------------------------------------*/
    pub fn table_groupby_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
    /*--------------------------------------------------------*/
    pub fn table_select_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
    /*--------------------------------------------------------*/
    pub fn table_orderby_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
    /*--------------------------------------------------------*/
    pub fn table_limit_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
    /*--------------------------------------------------------*/
    pub fn table_where_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
    /*--------------------------------------------------------*/
    pub fn table_join_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
    /*--------------------------------------------------------*/
    pub fn table_distinct_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
    //--------------------------------
    /*--------------------------------------------------------*/
    pub fn get_id(&self) -> i32 {
        self.id
    }
    /*--------------------------------------------------------*/
    //    pub fn colname_2_index(&self, colname: &str) -> usize {
    pub fn colname_2_index(&self, colname: &str) -> Result<usize, String> {
        match self.headers.iter().position(|r| r == colname) {
            Some(v) => Ok(v),
            None => Err(format!(
                "Column '{}' not found in table '{}' ",
                colname, self.table_name
            )),
        }
    }
    /*--------------------------------------------------------*/
    /*--------------------------------------------------------*/
    pub fn show_result(&self) -> Result<String, String> {
        Ok("GICK BRA".to_string())
    }
    pub fn show(&self) -> String {
        // calculate label widths
        let label_widths: Vec<usize> = self.headers.iter().map(|colvalue| colvalue.len()).collect();
        // calculate max width for each column
        let max_col_widths = self.rows.iter().fold(label_widths, |sofar, arow| {
            arow.iter()
                .enumerate()
                .map(|(i, acol)| {
                    if acol.len() > sofar[i] {
                        acol.len()
                    } else {
                        sofar[i].clone()
                    }
                })
                .collect()
        });
        /*------------------------------------------*/
        let build_topline = || -> String {
            format!(
                "+{}+",
                max_col_widths
                    .iter()
                    .map(|col_width| format!("-{}-", String::from("-".repeat(*col_width))))
                    .collect::<Vec<_>>()
                    .join("+")
            )
        };
        /*------------------------------------------*/
        let build_labels = || -> String {
            format!(
                "|{}|",
                self.headers
                    .iter()
                    .enumerate()
                    .map(|(i, lab)| format!(" {: ^1$} ", lab.clone(), max_col_widths[i]))
                    .collect::<Vec<_>>()
                    .join("|")
            )
        };
        /*------------------------------------------*/
        let build_rows = || -> String {
            self.rows
                .iter()
                .map(|arow| {
                    format!(
                        "|{}|",
                        arow.into_iter()
                            .enumerate()
                            .map(|(i, colval)| match colval.parse::<f64>() {
                                Ok(_) => format!(" {: >1$} ", colval.clone(), max_col_widths[i]),
                                Err(_) => format!(" {: <1$} ", colval.clone(), max_col_widths[i]),
                            })
                            .collect::<Vec<String>>()
                            .join("|")
                    )
                })
                .collect::<Vec<String>>()
                .join("\n")
        };
        /*------------------------------------------*/
        // FINAL RESULT

        return format!(
            "{topline}
{labels}
{topline}
{rows}
{topline}
",
            topline = build_topline(),
            labels = build_labels(),
            rows = build_rows()
        );
    }
}
