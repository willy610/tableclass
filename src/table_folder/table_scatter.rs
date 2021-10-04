use crate::logit::logit_gen_new_id;
use crate::table::{Table, TableRows};
use std::collections::btree_map::Entry::{Occupied, Vacant};
use std::collections::BTreeMap;

use std::collections::HashSet;
#[derive(Clone)]
pub struct Params2Scatter<'a> {
    pub row_y_from_column: &'a str,
    pub column_x_from_column: &'a str,
    pub value_from_column: &'a str,
}
/*
    Will pick 3 columns from in put rows
    and treat 
    -row_y_from_column as y
    -column_x_from_column as x
    -value_from_column as value at (x,y)
    - and produce soma kind of x-y plot
*/
impl Table {
    pub fn table_scatter(&self, par: Params2Scatter) -> String {
        let pick_index_for_rows = self
            .headers
            .iter()
            .position(|r| r == par.row_y_from_column)
            .unwrap();
        let pick_index_for_columns = self
            .headers
            .iter()
            .position(|r| r == par.column_x_from_column)
            .unwrap();
        let pick_index_for_values = self
            .headers
            .iter()
            .position(|r| r == par.value_from_column)
            .unwrap();

        let col_all_vals: HashSet<String> = self.rows.iter().map(|r| r[1].clone()).collect();
        let mut headers: Vec<String> = Vec::new();
        headers.push(format!(
            "{}/{}/{}",
            self.headers[pick_index_for_rows],
            self.headers[pick_index_for_columns],
            self.headers[pick_index_for_values]
        ));
        headers.append(
            &mut col_all_vals
                .iter()
                .map(|c| c.clone())
                .collect::<Vec<String>>(),
        );
        let mut new_rows: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();

        for arow in &self.rows {
            let key = arow[pick_index_for_rows].clone();
            let one_col_value = arow[pick_index_for_columns].clone();
            let the_scatter_value = arow[pick_index_for_values].clone();
            let val = match new_rows.entry(key.clone()) {
                Vacant(entry) => entry.insert(Vec::new()),
                Occupied(entry) => entry.into_mut(),
            };
            val.push((one_col_value, the_scatter_value));
        }
        let mut rows: TableRows = Vec::new();
        for (k, v) in new_rows {
            let mut that_row: Vec<String> = vec!["".to_string(); headers.len()];
            that_row[0] = k.clone();
            for (colname, ref value) in &v {
                let pos = headers.iter().position(|r| r == colname);
                that_row[pos.unwrap()] = value.to_string();
            }
            rows.push(that_row);
        }
        let new_id = logit_gen_new_id();
        let tbl2ret = Table::new(&self.table_name.clone(), headers.clone(), rows, new_id);
        tbl2ret.show()
    }
}
