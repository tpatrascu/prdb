use super::*;

#[test]
fn estimate_budget_exhaustion_falls_back_without_ending_the_planner_session() {
    let mut requests = 0;
    for _ in 0..DEFAULT_PLANNER_ESTIMATE_REQUESTS {
        assert!(
            crate::transaction::estimates::consume_planner_estimate_budget(
                &mut requests,
                DEFAULT_PLANNER_ESTIMATE_REQUESTS
            )
        );
    }
    assert_eq!(requests, DEFAULT_PLANNER_ESTIMATE_REQUESTS);
    assert!(
        !crate::transaction::estimates::consume_planner_estimate_budget(
            &mut requests,
            DEFAULT_PLANNER_ESTIMATE_REQUESTS
        )
    );
    assert_eq!(requests, DEFAULT_PLANNER_ESTIMATE_REQUESTS);

    for _ in 0..MAX_PLANNER_ESTIMATE_REQUESTS - DEFAULT_PLANNER_ESTIMATE_REQUESTS {
        assert!(
            crate::transaction::estimates::consume_planner_estimate_budget(
                &mut requests,
                MAX_PLANNER_ESTIMATE_REQUESTS
            )
        );
    }
    assert_eq!(requests, MAX_PLANNER_ESTIMATE_REQUESTS);
    assert!(
        !crate::transaction::estimates::consume_planner_estimate_budget(
            &mut requests,
            MAX_PLANNER_ESTIMATE_REQUESTS
        )
    );
}
