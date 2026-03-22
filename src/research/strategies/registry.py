from .atr_breakout import apply_atr_breakout, get_atr_breakout_candidates
from .breakout import apply_breakout, get_breakout_candidates
from .mean_reversion import apply_mean_reversion, get_mean_reversion_candidates
from .momentum_continuation import apply_momentum_continuation, get_momentum_continuation_candidates
from .trend_pullback import apply_trend_pullback, get_trend_pullback_candidates

STRATEGY_REGISTRY = {
    "breakout": {
        "apply": apply_breakout,
        "generate": get_breakout_candidates,
    },
    "mean_reversion": {
        "apply": apply_mean_reversion,
        "generate": get_mean_reversion_candidates,
    },
    "trend_pullback": {
        "apply": apply_trend_pullback,
        "generate": get_trend_pullback_candidates,
    },
    "atr_breakout": {
        "apply": apply_atr_breakout,
        "generate": get_atr_breakout_candidates,
    },
    "momentum_continuation": {
        "apply": apply_momentum_continuation,
        "generate": get_momentum_continuation_candidates,
    },
}
