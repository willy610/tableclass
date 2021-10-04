use crate::logit::{logit_collect_operlog_single, logit_gen_new_id};
use crate::table::{LogOper, LogSourceType, OperLog, SourceOrDerived, Table};
use cpu_time::ProcessTime;

#[derive(Clone, Debug)]
pub struct Params2Select<'a> {
    pub from_cols: Vec<&'a str>,
    pub to_table_name: Option<&'a str>,
    pub to_column_names: Option<Vec<&'a str>>,
}

impl Table {
    pub fn table_select(
        &self,
        sqlstm: Option<String>,
        par: Params2Select,
    ) -> Result<Table, String> {
        let start = ProcessTime::now();

        let mut pick_cols_index: Vec<usize> = Vec::new();

        for from_colname in &par.from_cols {
            match self.headers.iter().position(|h| h == from_colname) {
                Some(pos) => pick_cols_index.push(pos),
                None => {
                    return Err(format!(
                        "In 'table_select' the column '{}' not found in table '{}'",
                        from_colname, self.table_name
                    ));
                }
            }
        }
        let new_colnames = || -> Vec<String> {
            match &par.to_column_names {
                Some(names) => names.iter().map(|c| c.to_string()).collect(),
                None => pick_cols_index
                    .iter()
                    .map(|i| self.headers[*i].clone())
                    .collect(),
            }
        };
        let out_table_name = match par.to_table_name {
            Some(name) => name.to_string(),
            None => self.table_name.clone(),
        };
        let new_id = logit_gen_new_id();
        let tbl2ret = Table::new(
            &out_table_name.clone(),
            new_colnames(),
            self.rows
                .iter()
                .map(|arow| pick_cols_index.iter().map(|i| arow[*i].clone()).collect())
                .collect(),
            new_id,
        );
        let table_log = OperLog::new(
            LogOper::Select,
            self.table_name.to_string(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            sqlstm,
            vec![LogSourceType {
                sor_or_deriv: SourceOrDerived::Derived,
                intable_name: self.table_name.to_string(),
                intable_id: Some(self.get_id()),
            }],
        );
        logit_collect_operlog_single(table_log, new_id);
        Ok(tbl2ret)
    }
}
