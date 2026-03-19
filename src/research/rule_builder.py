from research.strategies.registry import STRATEGY_REGISTRY


def build_rule_candidates():
    all_candidates = []

    for name, strategy in STRATEGY_REGISTRY.items():
        candidates = strategy["generate"]()
        all_candidates.extend(candidates)

    return all_candidates
