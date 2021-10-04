use crate::logit::{
    is_logit_enabled, logit_collect_operlog_single, logit_disable, logit_enable, logit_gen_new_id,
};
use crate::table::{
    LogOper, LogSourceType, OperLog, SourceOrDerived, SubQueryFunction, Table, TableRow, TableRows,
};
use cpu_time::ProcessTime;
//////////////////////////////////////////////////////////////////
// to project with subquery
//

#[derive(Clone)]
pub enum SelectionKind {
    Literal {
        fixed: String,
    },
    Simple {
        from_colname: String,
    },
    SubQuery {
        the_sub_query_func: SubQueryFunction,
    },
}
pub struct Params2SelectSub<'a> {
    pub from_cols: Vec<SelectionKind>,
    pub to_table_name: Option<&'a str>,
    pub to_column_names: Vec<&'a str>,
    pub extra_tables: &'a Vec<&'a Table>,// tables required in the subquery
}
impl Table {
    /*--------------------------------------------------------*/
    pub fn table_select_sub(
        &self,
        sqlstm: Option<String>,
        par: Params2SelectSub,
    ) -> Result<Table, String> {
        let start = ProcessTime::now();
        let new_colnames: Vec<String> = par.to_column_names.iter().map(|c| c.to_string()).collect();
        let mut out_rows: TableRows = Vec::new();
        let mut log_subq: Vec<LogSourceType> = Vec::new();

        let logit_was_on_enabled = is_logit_enabled();
        let mut first_subq_done: bool = false;

        for a_row in &self.rows {
            let mut one_out_row: TableRow = Vec::new();

            for a_col in &par.from_cols {
                match a_col {
                    SelectionKind::Literal { fixed } => one_out_row.push(fixed.to_string()),
                    SelectionKind::Simple { from_colname } => {
                        let i = self.colname_2_index(&from_colname)?;
                        one_out_row.push(a_row[i].clone());
                    }
                    SelectionKind::SubQuery { the_sub_query_func } => {
                        let subq_col_value_result =
                            the_sub_query_func(self, &a_row, par.extra_tables)?;
                        if first_subq_done == false {
                            log_subq.push(LogSourceType {
                                sor_or_deriv: SourceOrDerived::Derived,
                                intable_name: self.table_name.to_string(),
                                intable_id: Some(subq_col_value_result.id.clone()),
                            })
                        }
                        if subq_col_value_result.rows.len() == 1
                            && subq_col_value_result.rows[0].len() == 1
                        {
                            one_out_row.push(subq_col_value_result.rows[0][0].clone());
                        } else {
                            return Err(format!("Subquery in a selection/projection must hold just one row with one value.
                            Was '{:?}'",subq_col_value_result.rows));
                        }
                    }
                }
            }
            // next row will stop logging (after logging the first row)
            // otherwise the log might be oveloaded
            first_subq_done = true;
            logit_disable();
            out_rows.push(one_out_row);
        }
        if logit_was_on_enabled {
            logit_enable();
        }

        let new_id = logit_gen_new_id();
        let out_table_name = match par.to_table_name {
            None => &self.table_name,
            Some(to_table_name) => to_table_name,
        };
        let tbl2ret = Table::new(out_table_name, new_colnames, out_rows, new_id);
        log_subq.push(LogSourceType {
            sor_or_deriv: SourceOrDerived::Derived,
            intable_name: self.table_name.to_string(),
            intable_id: Some(self.get_id()),
        });
        let table_log = OperLog::new(
            LogOper::SelectWithSub,
            self.table_name.to_string(),
            start.elapsed(),
            tbl2ret.rows.len(),
            tbl2ret.headers.len(),
            tbl2ret.id,
            sqlstm,
            log_subq,
        );
        logit_collect_operlog_single(table_log, new_id);
        Ok(tbl2ret)
    }
}
