use crate::logit::{
    is_logit_enabled, logit_collect_operlog_single, logit_disable, logit_enable, logit_gen_new_id,
};
use crate::table::{
    LogOper, LogSourceType, OperLog, SourceOrDerived, SubQueryFunction, Table, TableRow, TableRows,
};
use cpu_time::ProcessTime;

#[derive(Clone)]
pub enum OnceOrDepSub<'a> {
    OnceSub(Table),
    ADependentSubQuery {
        the_sub_query_func: SubQueryFunction,
        extra_tables: &'a Vec<&'a Table>,
    },
}
#[derive(Clone)]
pub struct SubOrOnceAndCheck<'a> {
    pub once_or_sub: OnceOrDepSub<'a>,
    pub the_eval_subq_result_function: fn(&Table, &TableRow, &Table) -> Result<bool, String>,
}

#[derive(Clone)]
pub enum SimpleOrComplex<'a> {
    SimpleCond(fn(&Table, &TableRow, Option<&Table>, Option<TableRow>) -> Result<bool, String>),
    ComplexCond(SubOrOnceAndCheck<'a>),
}
#[derive(Clone)]
pub struct Params2Where<'a> {
    pub super_obj: Option<&'a Table>,
    pub super_row: Option<TableRow>,
    pub simple_or_complex: SimpleOrComplex<'a>,
}
impl Table {
    pub fn table_where(&self, sqlstm: Option<String>, par: Params2Where) -> Result<Table, String> {
        let start = ProcessTime::now();
        let new_id = logit_gen_new_id();
        match par.simple_or_complex {
            SimpleOrComplex::SimpleCond(compare_condition) => {
                let mut outrows: TableRows = Vec::new();
                for arow in &self.rows {
                    if compare_condition(
                        self,
                        &arow.to_vec(),
                        par.super_obj,
                        par.super_row.clone(),
                    )? == true
                    {
                        outrows.push(arow.to_vec());
                    }
                }
                let tbl2ret = Table::new(
                    &self.table_name.clone(),
                    self.headers.clone(),
                    outrows,
                    new_id,
                );
                let where_log = OperLog::new(
                    LogOper::Where,
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
                logit_collect_operlog_single(where_log, new_id);
                Ok(tbl2ret)
            }
            SimpleOrComplex::ComplexCond(a_selectionkind) => {
                let mut outrows: TableRows = Vec::new();

                let logit_was_on_enabled = is_logit_enabled();
                let mut first_subq_done: bool = false;
                let mut log_id_from_dep_suq = 0;

                for arow in &self.rows {
                    match a_selectionkind.once_or_sub {
                        OnceOrDepSub::OnceSub(ref the_once_table) => {
                            // now apply custom function on result
                            let outcom = (a_selectionkind.the_eval_subq_result_function)(
                                self,
                                &arow,
                                &the_once_table,
                            )?;
                            if outcom {
                                outrows.push(arow.to_vec());
                            }
                            if first_subq_done == false {
                                first_subq_done = true;
                                log_id_from_dep_suq = the_once_table.id.clone();
                            }
                            logit_disable();
                        }
                        OnceOrDepSub::ADependentSubQuery {
                            the_sub_query_func,
                            extra_tables,
                        } => {
                            // Optimize hint
                            // the result is depending on value of columns from left only!
                            // Isolate the values and and insert the key with value true or false!
                            // So before calling the 'thesubqueryfun' consult the cache
                            // to se if we have an old result
                            let res = the_sub_query_func(self, &arow, extra_tables)?;
                            if first_subq_done == false {
                                first_subq_done = true;
                                log_id_from_dep_suq = res.id.clone();
                            }
                            logit_disable();
                            // now apply custom function on result
                            let outcom =
                                (a_selectionkind.the_eval_subq_result_function)(self, &arow, &res)?;
                            if outcom {
                                outrows.push(arow.to_vec());
                            }
                        }
                    }
                }
                if logit_was_on_enabled {
                    logit_enable();
                }
                let tbl2ret = Table::new(
                    &self.table_name.clone(),
                    self.headers.clone(),
                    outrows,
                    new_id,
                );
                let where_log = OperLog::new(
                    LogOper::WhereDepSub,
                    self.table_name.to_string(),
                    start.elapsed(),
                    tbl2ret.rows.len(),
                    tbl2ret.headers.len(),
                    tbl2ret.id,
                    sqlstm,
                    vec![
                        LogSourceType {
                            sor_or_deriv: SourceOrDerived::Derived,
                            intable_name: self.table_name.to_string(),
                            intable_id: Some(log_id_from_dep_suq),
                        },
                        LogSourceType {
                            sor_or_deriv: SourceOrDerived::Derived,
                            intable_name: self.table_name.to_string(),
                            intable_id: Some(self.get_id()),
                        },
                    ],
                );
                logit_collect_operlog_single(where_log, new_id);
                Ok(tbl2ret)
            }
        }
    }
}
