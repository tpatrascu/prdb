mod helpers;
macro_rules! forward_tests {
    ($mod:ident => { $($name:ident -> $ret:ty),* $(,)? }) => {
        $(
            #[test]
            fn $name() -> $ret {
                $mod::$name()
            }
        )*
    };
}

mod suite_01;
forward_tests!(suite_01 => {
    authenticated_metadata_and_admin_lifecycle_cross_gateway_and_supervisor_processes -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_02;
forward_tests!(suite_02 => {
    self_referencing_create_table_sql_crosses_gateway_and_supervisor_processes -> Result<(), Box<dyn std::error::Error>>,
    disconnected_submission_is_reconciled_after_gateway_restart -> Result<(), Box<dyn std::error::Error>>,
});

mod suite_03;
forward_tests!(suite_03 => {
    interactive_sql_ddl_and_dml_lifecycle -> Result<(), Box<dyn std::error::Error>>,
    interactive_sql_queries_expressions_joins_aggregates_and_ctes -> Result<(), Box<dyn std::error::Error>>,
    interactive_sql_runas_policies_and_error_boundaries -> Result<(), Box<dyn std::error::Error>>,
});
