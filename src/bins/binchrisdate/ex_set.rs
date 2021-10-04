use tableclass::explain::explain;
use tableclass::table::Table;

pub fn ex_set() {
    //
    // SET operations on from_str
    //
    let outer = || -> Result<Table, String> {
        let test_left = Table::from_str(
            "test_left",
            vec!["ETT", "TVA"],
            vec![vec!["1", "2"], vec!["3", "4"]],
        )?;
        eprintln!("test_left\n{}", test_left.show());

        let test_right = Table::from_str(
            "test_right",
            vec!["ETT", "TVA"],
            vec![vec!["1", "2"], vec!["31", "41"]],
        )?;
        eprintln!("test_right\n{}", test_right.show());

        let test_union = test_left.table_union(&test_right)?;
        eprintln!("test_left union test_right\n{}", test_union.show());

        let test_intersection = test_left.table_intersection(&test_right)?;
        eprintln!(
            "test_left intersection test_right\n{}",
            test_intersection.show()
        );
        Ok(test_intersection)
    };
    match outer() {
        Ok(result_set) => {
            eprintln!("{}", result_set.show());
            explain(&result_set, "set examples");
            // result_set is now computational
        }
        Err(err) => {
            eprintln!("Some error in 'ex_set' : {}", err);
        }
    }
}
