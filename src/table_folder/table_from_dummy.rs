use crate::table::Table;

impl Table {
    pub fn table_from_dummy() -> Table {
        let dummy = Table::new(
            "DUMMY",
            vec!["COLUMN_1".to_string()],
            vec![vec!["COLUMN_Value".to_string()]],
            0,
        );
        dummy
    }
    pub fn table_from_outline(&self, _sqlstm: &str, _par: &str) -> Table {
        self.clone()
    }
}
/*
let xxx = table_from_dummy()
.table_from("FROM CSV AAA","Params")
.table_join(Table::table_from("FROM CSV BBB"),"JOIN on AAA.PNO = BBB.PNO")
.table_where("AAA.X > 43 AND BBB.Y < 9")
.table_orderby("AAA.X")
.table_select("AAA.X, AAA.DATE, BBB.DATE");

xxx.show();
xxx.explain();

*/
