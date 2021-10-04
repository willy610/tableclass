use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table};
use cpu_time::ProcessTime;
use csv::StringRecord;

#[derive(Clone)]
pub struct Params2FromCSV<'a> {
    pub table_name: &'a str,
    pub dir: &'a str,
    pub file: &'a str,
    pub filter: Option<fn(&Vec<String>, &StringRecord) -> bool>,
    pub project: Option<Vec<&'a str>>,
}

impl Table {
    /*--------------------------------------------------------*/
    pub fn from_csv(par: Params2FromCSV) -> Result<Table, String> {
        let start = ProcessTime::now();

        let try_rdr = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(b',')
            .from_path(format!("{}/{}", par.dir, par.file));
        let mut rdr = match try_rdr {
            Err(x) => {
                return Err(format!(
                    "Table::from_csv failed on file='{}/{}'\n{}",
                    par.dir, par.file, x
                ))
            }
            Ok(found) => found,
        };
        let headers = rdr
            .headers()
            .unwrap()
            .iter()
            .map(|c| format!("{}", c))
            .collect::<Vec<String>>();

        let project_cols_index: Vec<usize> = match par.project {
            Some(ref names) => names
                .into_iter()
                .map(|c| headers.iter().position(|h| h == c).unwrap())
                .collect(),
            None => (0..headers.len() - 0).collect(),
        };
        let out_headers: Vec<String> = project_cols_index
            .iter()
            .map(|i| headers[*i].clone())
            .collect();

        let rows: Vec<Vec<String>> = rdr
            .records()
            .filter(|full_row| match par.filter {
                None => true,
                Some(ref filter_func) => filter_func(&headers, full_row.as_ref().unwrap()),
            })
            .map(|arow| {
                let arow = arow.unwrap();
                project_cols_index
                    .iter()
                    .map(|acol_index| arow[*acol_index].to_string())
                    .collect::<Vec<String>>()
            })
            .collect();
            
        let new_id = logit_gen_new_id();

        let ret_tbl = Table::new(&par.table_name.to_string(), out_headers, rows, new_id);
        let csv_source = OperLog::new(
            LogOper::FromCSV,
            par.table_name.to_string(),
            start.elapsed(),
            ret_tbl.rows.len(),
            ret_tbl.headers.len(),
            ret_tbl.id,
            Some(format!("FROM csv '{}'", par.file)),
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Source,
                intable_name: format!("{}/{}", par.dir, par.file).to_string(),
                intable_id: None,
            }],
        );
        logit_collect_operlog_single(csv_source, new_id);
        Ok(ret_tbl)
    }
    /*--------------------------------------------------------*/
    pub fn from_str(
        table_name: &str,
        headers: Vec<&str>,
        rows: Vec<Vec<&str>>,
    ) -> Result<Table, String> {
        let start = ProcessTime::now();
        for r in &rows {
            if r.len() != headers.len() {
                eprintln!("Row of other size then header{:?}", r);
            }
        }
        let new_id = logit_gen_new_id();
        let tbl2ret = Table::new(
            &table_name.to_string(),
            headers.iter().map(|x| x.to_string()).collect(),
            rows.iter()
                .map(|row| row.iter().map(|column| column.to_string()).collect())
                .collect(),
            new_id,
        );
        let from_str_log = OperLog::new(
            LogOper::FromStr,
            table_name.to_string(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            Some("FROM 'str'".to_string()),
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Source,
                intable_name: "from_str".to_string(),
                intable_id: None,
            }],
        );
        logit_collect_operlog_single(from_str_log, new_id);
        Ok(tbl2ret)
    }
}
