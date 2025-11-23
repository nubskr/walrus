import time
import pytest
from test_cluster import (
    NODES,
    ensure_cluster_ready,
    produce_and_assert_ok,
    list_generations,
    send_frame,
    make_internal_state_request,
    decode_internal_state,
    force_rollover_to_generation
)

def get_partition_leader(node_id, topic="logs", partition=0):
    resp = send_frame(*NODES[node_id], make_internal_state_request([topic]))
    if not resp:
        return None
    state = decode_internal_state(resp)
    if not state:
        return None
    for t in state["topics"]:
        if t["name"] == topic:
            for p in t["partitions"]:
                if p["partition"] == partition:
                    return int(p["leader"])
    return None

def test_rollover_rotates_leadership():
    """
    Verify that when a partition rolls over, leadership is transferred to the next node.
    """
    ensure_cluster_ready()
    
    # 1. Identify initial leader
    initial_leader = get_partition_leader(1)
    assert initial_leader is not None
    print(f"Initial leader: {initial_leader}")

    # 2. Force rollover to generation 2
    # This writes data until the generation counter increments
    force_rollover_to_generation(2)
    
    # Generation 2 is bootstrapped on Node 1 (in bootstrap_node_one).
    # So we expect it to be 1. We need to force *another* rollover to trigger Monitor logic.
    
    force_rollover_to_generation(3)

    # 3. Identify new leader for Gen 3
    new_leader = get_partition_leader(1)
    assert new_leader is not None
    print(f"New leader (Gen 3): {new_leader}")

    # 4. Assert rotation occurred
    # Since we have 3 nodes [1, 2, 3], if we start at 1, we expect 2.
    assert new_leader != initial_leader, f"Leader did not rotate! Still {initial_leader}"
    
    expected_leader = (initial_leader % 3) + 1
    assert new_leader == expected_leader, f"Expected leader {expected_leader}, got {new_leader}"
    
    # 5. Force another rollover to generation 4 (Optional verification)
    # force_rollover_to_generation(4)
    
    # 6. Check again
    # final_leader = get_partition_leader(1)
    # assert final_leader != new_leader
    # expected_final = (new_leader % 3) + 1
    # assert final_leader == expected_final, f"Expected leader {expected_final}, got {final_leader}"
